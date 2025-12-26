"""
AI Chat Service for ScholarStream Assistant
Real-time conversational AI powered by Gemini
"""
import google.generativeai as genai
import json
from typing import Dict, Any, List, Optional
import structlog
from datetime import datetime, timedelta

from app.models import UserProfile
from app.database import db
from app.config import settings

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
                
                # TRANSPARENCY: Show filtering results
                thinking_process.append(f"\n📊 **Search Results:**")
                thinking_process.append(f"- Total opportunities scanned: **{search_stats['total_scanned']}**")
                thinking_process.append(f"- Expired (filtered out): {search_stats['expired']}")
                thinking_process.append(f"- Location mismatch (filtered out): {search_stats['location_filtered']}")
                thinking_process.append(f"- Type mismatch (filtered out): {search_stats['type_filtered']}")
                if search_stats['urgency_filtered'] > 0:
                    thinking_process.append(f"- Urgency mismatch (filtered out): {search_stats['urgency_filtered']}")
                thinking_process.append(f"- **✅ Final matches: {len(opportunities)}**")
                
                # Add opportunities to prompt for AI context
                if opportunities:
                    system_prompt += f"\n\nSEARCH RESULTS ({len(opportunities)} found):\n"
                    # Limit to top 3 for Emergency to reduce cognitive load
                    limit = 3 if is_emergency else 5
                    for i, opp in enumerate(opportunities[:limit], 1):
                        system_prompt += f"\n{i}. **{opp.get('name')}** - ${opp.get('amount', 0):,}\n"
                        system_prompt += f"   - Type: {opp.get('type')}\n"
                        system_prompt += f"   - Deadline: {opp.get('deadline')}\n"
                        system_prompt += f"   - Location: {opp.get('location_eligibility')}\n"
                        system_prompt += f"   - Match Score: {opp.get('match_score')}%\n"
            
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
            
            # FORMATTING: Prepend thinking process as structured markdown
            if thinking_process:
                thinking_section = "\n".join(thinking_process)
                ai_message = f"## 🧠 My Thinking Process\n\n{thinking_section}\n\n---\n\n## 💡 My Recommendation\n\n{ai_message}"
            
            # Save conversation
            await self._save_message(user_id, "user", message)
            await self._save_message(user_id, "assistant", ai_message)
            
            return {
                'message': ai_message,
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
        if 'hackathon' in message_lower:
            criteria['types'].append('hackathon')
        if 'bounty' in message_lower or 'bounties' in message_lower:
            criteria['types'].append('bounty')
        if 'scholarship' in message_lower:
            criteria['types'].append('scholarship')
        if 'competition' in message_lower:
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
        
        return criteria
    
    async def _search_opportunities_with_stats(
        self, 
        criteria: Dict[str, Any], 
        profile: Dict
    ) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
        Search opportunities with detailed statistics for transparency
        Returns: (opportunities_list, statistics_dict)
        """
        stats = {
            'total_scanned': 0,
            'expired': 0,
            'location_filtered': 0,
            'type_filtered': 0,
            'urgency_filtered': 0
        }
        
        try:
            # Get all opportunities from database
            all_opps = await db.get_all_scholarships()
            stats['total_scanned'] = len(all_opps)
            
            filtered_opps = []
            now = datetime.now()
            
            user_country = (profile.get('country') or 'United States').lower()
            user_state = (profile.get('state') or '').lower()
            
            for opp in all_opps:
                # 1. STRICT EXPIRATION CHECK (with grace period for today)
                if opp.deadline:
                    try:
                        deadline_date = datetime.fromisoformat(opp.deadline.replace('Z', '+00:00'))
                        # Filter if deadline was yesterday or earlier
                        if deadline_date.date() < now.date():
                            stats['expired'] += 1
                            continue
                    except ValueError:
                        stats['expired'] += 1
                        continue
                
                # 2. TYPE FILTER
                opp_type = self._infer_type(opp)
                if criteria.get('types') and opp_type not in criteria['types']:
                    stats['type_filtered'] += 1
                    continue
                
                # 3. LOCATION FILTER (Enhanced)
                eligibility = getattr(opp, 'eligibility', {})
                if hasattr(eligibility, 'dict'):
                    eligibility = eligibility.dict()
                
                opp_states = [s.lower() for s in (eligibility.get('states') or [])]
                opp_citizenship = (eligibility.get('citizenship') or '').lower()
                
                # State restriction check
                if opp_states and user_state:
                    if not any(user_state in s for s in opp_states):
                        stats['location_filtered'] += 1
                        continue
                
                # Citizenship restriction check
                if opp_citizenship and opp_citizenship != 'any':
                    if user_country not in opp_citizenship and 'international' not in opp_citizenship:
                        stats['location_filtered'] += 1
                        continue
                
                # 4. URGENCY FILTER
                urgency = criteria.get('urgency', 'any')
                if urgency != 'any' and opp.deadline:
                    try:
                        deadline_date = datetime.fromisoformat(opp.deadline.replace('Z', '+00:00'))
                        days_until = (deadline_date - now).days
                        
                        if urgency == 'immediate' and days_until > 2:
                            stats['urgency_filtered'] += 1
                            continue
                        if urgency == 'this_week' and days_until > 7:
                            stats['urgency_filtered'] += 1
                            continue
                        if urgency == 'this_month' and days_until > 30:
                            stats['urgency_filtered'] += 1
                            continue
                    except:
                        continue
                
                filtered_opps.append(opp)
            
            # Convert to dict format
            results = []
            for opp in filtered_opps[:20]:  # Limit to 20
                results.append({
                    'id': opp.id,
                    'name': opp.name,
                    'organization': opp.organization,
                    'amount': opp.amount,
                    'amount_display': opp.amount_display,
                    'deadline': opp.deadline,
                    'type': self._infer_type(opp),
                    'match_score': opp.match_score,
                    'source_url': opp.source_url,
                    'tags': opp.tags,
                    'description': opp.description,
                    'location_eligibility': self._get_location_string(opp),
                    'priority_level': opp.priority_level
                })
            
            # Sort by match score
            results.sort(key=lambda x: x.get('match_score', 0), reverse=True)
            
            logger.info(
                "Search completed with stats",
                total_scanned=stats['total_scanned'],
                final_matches=len(results),
                expired_filtered=stats['expired'],
                location_filtered=stats['location_filtered']
            )
            
            return results, stats
            
        except Exception as e:
            logger.error("Search failed", error=str(e))
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
