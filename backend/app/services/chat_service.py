"""
AI Chat Service for ScholarStream Assistant
Real-time conversational AI powered by Gemini
"""
import google.generativeai as genai
import json
from typing import Dict, Any, List, Optional
import structlog
from datetime import datetime, timedelta

from app.models import UserProfile, Scholarship
from app.database import db
from app.config import settings
from app.services.matching_service import matching_service
from app.services.personalization_engine import personalization_engine
from app.services.cortex.navigator import scout

logger = structlog.get_logger()


class ChatService:
    """AI Chat Assistant powered by Gemini"""
    
    def __init__(self):
        """Initialize Gemini using settings"""
        if not settings.gemini_api_key:
            raise Exception("GEMINI_API_KEY not configured in settings")
        
        genai.configure(api_key=settings.gemini_api_key)
        self.model = genai.GenerativeModel(settings.gemini_model)
        logger.info("Chat service initialized", model=settings.gemini_model)
    
    async def chat(
        self,
        user_id: str,
        message: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process chat message with enhanced transparency and markdown formatting
        """
        try:
            # Build context-rich prompt
            system_prompt = self._build_system_prompt(context)
            
            # Check for Emergency Mode
            is_emergency = self._detect_emergency_mode(message)
            
            # FIX: Detect if user wants to search for opportunities
            needs_search = self._detect_search_intent(message)
            
            opportunities = []
            thinking_process = []
            search_stats = None
            
            if needs_search or is_emergency:
                # TRANSPARENCY: Log search process
                if is_emergency:
                    thinking_process.append("🚨 **EMERGENCY MODE ACTIVATED**")
                    thinking_process.append("- prioritizing deadlines < 14 days")
                    thinking_process.append("- prioritizing quick-apply formats")
                else:
                    thinking_process.append("🔍 **Analyzing your request...**")
                
                # Extract search criteria
                search_criteria = await self._extract_search_criteria(message, context.get('user_profile', {}))
                
                # Override for Emergency
                if is_emergency:
                    search_criteria['urgency'] = 'immediate'
                    
                thinking_process.append(f"\n📋 **Search Criteria Identified:**")
                thinking_process.append(f"- **Types**: {', '.join(search_criteria['types'])}")
                if search_criteria['urgency'] != 'any':
                    thinking_process.append(f"- **Urgency**: {search_criteria['urgency']}")
                thinking_process.append(f"- **Location**: {context.get('user_profile', {}).get('state', 'Any')}, {context.get('user_profile', {}).get('country', 'Any')}")
                
                # Search with detailed statistics
                opportunities, search_stats = await self._search_opportunities_with_stats(
                    search_criteria, 
                    context.get('user_profile', {})
                )
                
                # Check if it was broadened
                if search_criteria.get('broadened'):
                    thinking_process.append("\n⚠️ **Broadening search to find more potential matches...**")
                
                # TRANSPARENCY: Show filtering results
                thinking_process.append(f"\n📊 **Search Results:**")
                thinking_process.append(f"- Total opportunities scanned: **{search_stats['total_scanned']}**")
                thinking_process.append(f"- Expired (filtered out): {search_stats['expired']}")
                thinking_process.append(f"- Location mismatch (filtered out): {search_stats['location_filtered']}")
                thinking_process.append(f"- Type mismatch (filtered out): {search_stats['type_filtered']}")
                if search_stats.get('urgency_filtered', 0) > 0:
                    thinking_process.append(f"- Urgency mismatch (filtered out): {search_stats['urgency_filtered']}")
                thinking_process.append(f"- **✅ Final matches: {len(opportunities)}**")
                
                # Add opportunities to prompt for AI context
                if opportunities:
                    system_prompt += f"\n\nSEARCH RESULTS ({len(opportunities)} found):\n"
                    # UPGRADED: Show MORE results in emergency to give the student more options
                    limit = 10 if is_emergency else 8
                    for i, opp in enumerate(opportunities[:limit], 1):
                        system_prompt += f"\n{i}. **{opp.get('name')}** - {opp.get('amount_display', '$0')}\n"
                        system_prompt += f"   - Organization: {opp.get('organization')}\n"
                        system_prompt += f"   - Match Score: {opp.get('match_score')}%\n"
                        system_prompt += f"   - Deadline: {opp.get('deadline') or 'Check listing'}\n"
                        system_prompt += f"   - Link: {opp.get('source_url')}\n"
                        system_prompt += f"   - Type: {opp.get('type')}\n"
                        system_prompt += f"   - Location: {opp.get('location_eligibility')}\n"
            
            # Generate AI response with enhanced prompt
            prompt_suffix = "\n\nUSER MESSAGE: {message}\n\nProvide a helpful, well-formatted markdown response:"
            if is_emergency:
                 prompt_suffix = """
                 \n\nUSER MESSAGE: {message}
                 \n\nCRITICAL INSTRUCTION - EMERGENCY MODE:
                 The user is stressed. Use the 'Empathy Sandwich' technique:
                 1. Top Slice: Validate their stress briefly ("I hear you, and we can fix this.").
                 2. Meat: Present the solution clearly and actionably.
                 3. Bottom Slice: Reassure them ("You've got this.").
                 Keep it concise.
                 """
            
            full_prompt = system_prompt + prompt_suffix.format(message=message)
            
            response = await self.model.generate_content_async(full_prompt)
            ai_message = response.text
            
            # V2 FIX: Separate thinking process from recommendation for better UX
            # The frontend will render these as separate collapsible sections
            thinking_section = ""
            if thinking_process:
                thinking_section = "\n".join(thinking_process)
            
            # Save conversation
            await self._save_message(user_id, "user", message)
            await self._save_message(user_id, "assistant", ai_message)
            
            return {
                'message': ai_message,
                'thinking_process': thinking_section,  # V2: Separate for frontend streaming
                'opportunities': opportunities[:10] if opportunities else [],
                'actions': self._generate_actions(opportunities, message),
                'search_stats': search_stats
            }
            
        except Exception as e:
            logger.error("Chat failed", error=str(e))
            return {
                'message': "❌ I encountered an error while processing your request. Please try rephrasing your question or contact support if the issue persists.",
                'opportunities': [],
                'actions': [],
                'search_stats': None
            }
    
    def _detect_emergency_mode(self, message: str) -> bool:
        """Detect high-stress/urgent keywords"""
        triggers = [
            "urgent", "emergency", "deadline", "asap", "failed", "broke", 
            "need money now", "help me", "immediately", "today", "tomorrow"
        ]
        return any(t in message.lower() for t in triggers)

    def _build_system_prompt(self, context: Dict[str, Any]) -> str:
        """Build context-rich system prompt"""
        profile = context.get('user_profile', {})
        page = context.get('current_page', 'unknown')
        
        prompt = f"""You are ScholarStream Assistant, an expert financial opportunity advisor for students.

STUDENT PROFILE:
- Name: {profile.get('name', 'Student')}
- Major: {profile.get('major', 'Unknown')}
- Location: {profile.get('city', '')}, {profile.get('state', '')}, {profile.get('country', 'United States')}
- Interests: {', '.join(profile.get('interests', []))}

YOUR GOAL:
Provide accurate, trustworthy, and transparent assistance. You MUST explain your thought process.

RESPONSE GUIDELINES:
1. **Transparency**: Always start by briefly explaining what you searched for or how you analyzed the request.
2. **Accuracy**: NEVER hallucinate opportunities. Only discuss the ones provided in the SEARCH RESULTS section.
3. **Formatting**: Use Markdown effectively.
   - Use **bold** for key terms.
   - Use bullet points for lists.
   - Use > blockquotes for important tips.
4. **Tone**: Professional yet encouraging.

If SEARCH RESULTS are provided:
- Summarize the top 2-3 matches.
- Explain WHY they match the student's profile (e.g., "Matches your location in {profile.get('state', 'your area')}" or "Aligns with your interest in {profile.get('major', 'your major')}").
- Mention the deadline clearly.

IMPORTANT: You HAVE access to these opportunities. They are from our internal database. DO NOT say you cannot search or access external databases. Use the provided SEARCH RESULTS as your source of truth.

If NO SEARCH RESULTS are found:
- Be honest that our *current database* doesn't have matches, but suggest broadening the search or checking back later.
"""
        return prompt
    
    def _detect_search_intent(self, message: str) -> bool:
        """Detect if user wants to search for opportunities"""
        intent_keywords = [
            'find', 'search', 'show me', 'need money', 'urgent', 'hackathon',
            'scholarship', 'bounty', 'competition', 'opportunity', 'apply',
            'deadline', 'this week', 'today', 'tomorrow', 'help me find',
            'looking for', 'need', 'want'
        ]
        message_lower = message.lower()
        return any(keyword in message_lower for keyword in intent_keywords)
    
    async def _extract_search_criteria(self, message: str, profile: Dict) -> Dict[str, Any]:
        """Extract search parameters from natural language"""
        message_lower = message.lower()
        
        criteria = {
            'types': [],
            'urgency': 'any',
            'min_amount': None,
            'keywords': []
        }
        
        # Detect types
        if any(word in message_lower for word in ['hackathon', 'build', 'sprint']):
            criteria['types'].append('hackathon')
        if any(word in message_lower for word in ['bounty', 'bug', 'issue']):
            criteria['types'].append('bounty')
        if any(word in message_lower for word in ['scholarship', 'grant', 'funding']):
            criteria['types'].append('scholarship')
        if any(word in message_lower for word in ['competition', 'contest', 'challenge']):
            criteria['types'].append('competition')
        
        # Default to all types if none specified
        if not criteria['types']:
            criteria['types'] = ['scholarship', 'hackathon', 'bounty', 'competition']
        
        # Detect urgency
        if any(word in message_lower for word in ['urgent', 'now', 'today', 'asap', 'immediately']):
            criteria['urgency'] = 'immediate'
        elif any(word in message_lower for word in ['this week', 'soon', 'quickly']):
            criteria['urgency'] = 'this_week'
        elif any(word in message_lower for word in ['this month', 'month']):
            criteria['urgency'] = 'this_month'
        
        # Extract keywords from the profile to enrich search
        interests = profile.get('interests', [])
        if interests:
            criteria['keywords'].extend(interests[:3])  # Top 3 interests as search keywords
        
        return criteria
    
    async def _search_opportunities_with_stats(
        self, 
        criteria: Dict[str, Any], 
        profile: Dict,
        depth: int = 0
    ) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
<<<<<<< Updated upstream
        Search opportunities with detailed statistics for transparency
        V2 FIXES:
        - Recalculate match scores in real-time
        - Broadened location logic (global = accessible)
        - Software devs match hackathons/bounties
        Returns: (opportunities_list, statistics_dict)
=======
        Search opportunities with detailed statistics and real-time matching
        Recursive broadening for emergency situations.
>>>>>>> Stashed changes
        """
        from app.services.personalization_engine import personalization_engine
        from app.models import UserProfile
        
        stats = {
            'total_scanned': 0,
            'expired': 0,
            'location_filtered': 0,
            'type_filtered': 0,
            'urgency_filtered': 0
        }
        
        try:
            # 1. Start with Database Search
            all_opps = await db.get_all_scholarships()
            stats['total_scanned'] = len(all_opps)
            
            # TRIGGER ON-DEMAND SCRAPING if database is thin
            if depth == 0 and (len(all_opps) < 20 or any(kw in criteria['types'] for kw in ['hackathon', 'bounty'])):
                logger.info("Triggering background on-demand search mission")
                search_query = f"{' '.join(criteria['types'])} {profile.get('major', '')} {profile.get('school', '')} {profile.get('interests', [''])[0]}"
                asyncio.create_task(scout.execute_mission(search_query))
            
            filtered_opps = []
            now = datetime.now()
            user_profile_obj = UserProfile(**profile) if profile else None
            
<<<<<<< Updated upstream
            user_country = (profile.get('country') or '').lower()
=======
            # Detect Nigeran context for UI/Ibadan
            school_str = str(profile.get('school', '')).lower()
            is_nigeria = 'nigeria' in str(profile.get('country', '')).lower() or 'ibadan' in school_str or 'nigeria' in school_str
            
            user_country = (profile.get('country') or ('Nigeria' if is_nigeria else 'United States')).lower()
>>>>>>> Stashed changes
            user_state = (profile.get('state') or '').lower()
            user_interests = [i.lower() for i in (profile.get('interests') or [])]
            
            # Build UserProfile object for scoring
            user_profile_obj = None
            try:
                user_profile_obj = UserProfile(**profile) if profile else None
            except Exception:
                pass
            
            for opp in all_opps:
<<<<<<< Updated upstream
                # 1. EXPIRATION CHECK (lenient - include today)
                if opp.deadline:
                    try:
                        deadline_date = datetime.fromisoformat(opp.deadline.replace('Z', '+00:00'))
                        # Filter only if deadline was BEFORE today
                        if deadline_date.date() < now.date():
                            stats['expired'] += 1
                            continue
                    except (ValueError, AttributeError):
                        # If we can't parse deadline, assume valid
                        pass
                
                # 2. TYPE FILTER (BROADENED: Software devs match hackathons/bounties/competitions)
=======
                # STRICT EXPIRATION CHECK
                if opp.deadline:
                    try:
                        deadline_date = datetime.fromisoformat(opp.deadline.replace('Z', '+00:00'))
                        if deadline_date.date() < now.date() - timedelta(days=1): # 1 day grace for timezones
                            stats['expired'] += 1
                            continue
                    except:
                        pass
                
                # TYPE FILTER
>>>>>>> Stashed changes
                opp_type = self._infer_type(opp)
                requested_types = criteria.get('types') or []
                
<<<<<<< Updated upstream
                # V2 FIX: If user has software/coding/tech interests, auto-include hackathons/bounties
                tech_keywords = ['software', 'coding', 'programming', 'developer', 'tech', 'ai', 'web', 'mobile', 'computer']
                user_is_tech = any(kw in ' '.join(user_interests) for kw in tech_keywords)
                
                if requested_types:
                    if opp_type not in requested_types:
                        # V2: Tech users automatically qualify for hackathons/bounties
                        if not (user_is_tech and opp_type in ['hackathon', 'bounty', 'competition']):
                            stats['type_filtered'] += 1
                            continue
                
                # 3. LOCATION FILTER (V2: VERY LENIENT for global opportunities)
                eligibility = getattr(opp, 'eligibility', {})
                if hasattr(eligibility, 'dict'):
                    eligibility = eligibility.dict()
                elif hasattr(eligibility, 'model_dump'):
                    eligibility = eligibility.model_dump()
                
                opp_states = [s.lower() for s in (eligibility.get('states') or [])]
                opp_citizenship = (eligibility.get('citizenship') or '').lower()
                geo_tags = [t.lower() for t in (getattr(opp, 'geo_tags', []) or [])]
                
                # V2 FIX: Global/Remote/International opportunities accessible to ALL
                is_global = any(tag in geo_tags for tag in ['global', 'remote', 'international', 'online', 'worldwide'])
                is_open_citizenship = not opp_citizenship or opp_citizenship in ['any', 'international', 'all']
                
                # Only filter if explicitly restricted AND user doesn't match
                location_restricted = False
                if opp_states and user_state:
                    if not any(user_state in s for s in opp_states):
                        if not is_global:
                            location_restricted = True
                
                if opp_citizenship and opp_citizenship not in ['any', 'international', 'all']:
                    if user_country and user_country not in opp_citizenship:
                        if not is_global:
                            location_restricted = True
                
                # V2: Hackathons/Bounties are almost always global
                if opp_type in ['hackathon', 'bounty', 'competition']:
                    location_restricted = False
                
                if location_restricted:
                    stats['location_filtered'] += 1
                    continue
                
                # 4. URGENCY FILTER (unchanged)
=======
                # LOCATION FILTER
                location_str = self._get_location_string(opp).lower()
                is_global = any(kw in location_str for kw in ['global', 'international', 'worldwide', 'anywhere', 'remote'])
                
                if not is_global:
                    if user_country not in location_str and user_state not in location_str:
                        # Hard match for Nigeria/Africa keywords
                        if not (is_nigeria and any(kw in location_str for kw in ['nigeria', 'africa', 'developing'])):
                            stats['location_filtered'] += 1
                            continue
                
                # URGENCY FILTER
>>>>>>> Stashed changes
                urgency = criteria.get('urgency', 'any')
                if urgency != 'any' and opp.deadline:
                    try:
                        deadline_date = datetime.fromisoformat(opp.deadline.replace('Z', '+00:00'))
                        days_until = (deadline_date - now).days
                        
<<<<<<< Updated upstream
                        if urgency == 'immediate' and days_until > 7:
=======
                        if urgency == 'immediate' and days_until > 10: # Expanded from 7 to 10
>>>>>>> Stashed changes
                            stats['urgency_filtered'] += 1
                            continue
                        if urgency == 'this_week' and days_until > 20: # Expanded from 14 to 20
                            stats['urgency_filtered'] += 1
                            continue
                    except:
                        pass
                
                filtered_opps.append(opp)
            
<<<<<<< Updated upstream
            # V2 FIX: Real-time match score recalculation
            results = []
            for opp in filtered_opps[:30]:  # Limit to 30
                # Build opportunity dict for personalization engine
                opp_dict = {
                    'name': opp.name or '',
                    'title': opp.name or '',
                    'description': opp.description or '',
                    'organization': opp.organization or '',
                    'tags': opp.tags or [],
                    'geo_tags': getattr(opp, 'geo_tags', []) or [],
                    'eligibility': {},
                    'requirements': {}
                }
                
                # Get eligibility dict
                if hasattr(opp, 'eligibility'):
                    if hasattr(opp.eligibility, 'model_dump'):
                        opp_dict['eligibility'] = opp.eligibility.model_dump()
                    elif hasattr(opp.eligibility, 'dict'):
                        opp_dict['eligibility'] = opp.eligibility.dict()
                    elif isinstance(opp.eligibility, dict):
                        opp_dict['eligibility'] = opp.eligibility
                
                # V2 FIX: Calculate fresh match score
                fresh_score = 50  # Default
                if user_profile_obj:
                    try:
                        fresh_score = personalization_engine.calculate_personalized_score(opp_dict, user_profile_obj)
                    except Exception as e:
                        logger.warning("Score calc failed", error=str(e))
                        fresh_score = opp.match_score or 50
                elif opp.match_score:
                    fresh_score = opp.match_score
=======
            # REAL-TIME PERSONALIZATION & RANKING
            results = []
            for opp in filtered_opps:
                real_score = personalization_engine.calculate_personalized_score(opp.model_dump(), user_profile_obj)
>>>>>>> Stashed changes
                
                results.append({
                    'id': opp.id,
                    'name': opp.name,
                    'organization': opp.organization,
                    'amount': opp.amount,
                    'amount_display': opp.amount_display,
                    'deadline': opp.deadline,
                    'type': self._infer_type(opp),
<<<<<<< Updated upstream
                    'match_score': int(round(fresh_score)),  # V2: Fresh score
=======
                    'match_score': int(real_score),
>>>>>>> Stashed changes
                    'source_url': opp.source_url,
                    'tags': opp.tags,
                    'description': opp.description,
                    'location_eligibility': self._get_location_string(opp),
                    'priority_level': opp.priority_level
                })
            
<<<<<<< Updated upstream
            # Sort by fresh match score
=======
            # SORT BY MATCH SCORE
>>>>>>> Stashed changes
            results.sort(key=lambda x: x.get('match_score', 0), reverse=True)
            results = results[:15]
            
<<<<<<< Updated upstream
            logger.info(
                "Search V2 completed",
                total_scanned=stats['total_scanned'],
                final_matches=len(results),
                expired_filtered=stats['expired'],
                location_filtered=stats['location_filtered']
            )
            
=======
            # EMERGENCY RECOVERY: If 0 results for crisis, broaden and retry ONCE
            if not results and depth == 0 and (criteria.get('urgency') != 'any' or criteria.get('location') != 'any'):
                logger.info("CRISIS RECOVERY: Broadening search criteria")
                broader_criteria = criteria.copy()
                broader_criteria['urgency'] = 'any'
                broader_criteria['broadened'] = True # Flag for the thinking process
                return await self._search_opportunities_with_stats(broader_criteria, profile, depth=1)
                
>>>>>>> Stashed changes
            return results, stats
            
        except Exception as e:
            logger.error("Search failed", error=str(e))
            import traceback
            traceback.print_exc()
            return [], stats
    
    def _infer_type(self, opp) -> str:
        """Infer opportunity type from tags/description"""
        tags_str = ' '.join(opp.tags or []).lower()
        desc_str = (opp.description or '').lower()
        combined = f"{tags_str} {desc_str}"
        
        if 'hackathon' in combined or 'hack' in combined:
            return 'hackathon'
        elif 'bounty' in combined or 'bug' in combined:
            return 'bounty'
        elif 'competition' in combined or 'contest' in combined:
            return 'competition'
        else:
            return 'scholarship'
    
    def _calculate_urgency(self, opp) -> str:
        """Calculate urgency from deadline"""
        if not opp.deadline:
            return 'immediate'  # Rolling deadline
        
        try:
            deadline_date = datetime.fromisoformat(opp.deadline.replace('Z', '+00:00'))
            days_until = (deadline_date - datetime.now()).days
            
            if days_until <= 2:
                return 'immediate'
            elif days_until <= 7:
                return 'this_week'
            elif days_until <= 30:
                return 'this_month'
            else:
                return 'future'
        except:
            return 'future'
            
    def _get_location_string(self, opp) -> str:
        """Get human readable location eligibility"""
        eligibility = getattr(opp, 'eligibility', {})
        if hasattr(eligibility, 'dict'):
            eligibility = eligibility.dict()
            
        states = eligibility.get('states') or []
        citizenship = eligibility.get('citizenship')
        
        if states:
            return f"Residents of {', '.join(states)}"
        if citizenship and citizenship.lower() != 'any':
            return f"Citizens of {citizenship}"
        return "Open / International"
    
    def _generate_actions(self, opportunities: List[Dict], message: str) -> List[Dict[str, Any]]:
        """Generate suggested actions"""
        actions = []
        
        if opportunities:
            # Add save action for top opportunities
            for opp in opportunities[:3]:
                actions.append({
                    'type': 'save',
                    'opportunity_id': opp.get('id'),
                    'label': f"Save {opp.get('name', 'opportunity')}"
                })
        
        return actions
    
    async def _save_message(self, user_id: str, role: str, content: str):
        """Save conversation to database"""
        try:
            # Save to Firebase (implement in db.py)
            await db.save_chat_message(user_id, role, content)
        except Exception as e:
            logger.error("Failed to save message", error=str(e))


# Global chat service instance
chat_service = ChatService()
