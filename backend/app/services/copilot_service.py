
import structlog
from typing import Dict, Any, List, Optional
import json
from urllib.parse import urlparse

from app.services.ai_service import ai_service
from app.config import settings

logger = structlog.get_logger()


class CopilotService:
    """
    ScholarStream Co-Pilot V2: Context-Aware Application Assistant
    
    KEY UPGRADES:
    1. Platform Detection: Knows if user is on DevPost, DoraHacks, etc.
    2. Tri-Fold Knowledge Base: User Profile + Uploaded Docs + Page Context
    3. Platform-Specific Coaching: Tailored advice for each application platform
    """
    
    # Platform-specific expert personas
    PLATFORM_PERSONAS = {
        'devpost.com': {
            'name': 'DevPost Hackathon Coach',
            'expertise': 'DevPost hackathon submissions, project showcases, team formation',
            'tips': [
                'Focus on the "What it does" section - judges skim this first',
                'Include a demo video - projects with videos get 3x more views',
                'Tag technologies accurately for sponsor prizes',
                'Highlight the "How we built it" with specific tech challenges overcome'
            ]
        },
        'dorahacks.io': {
            'name': 'DoraHacks BUIDL Expert',
            'expertise': 'DoraHacks BUIDLs, Web3 hackathons, blockchain projects',
            'tips': [
                'Emphasize on-chain components and smart contract innovation',
                'Include GitHub repo and deployed contract addresses',
                'Focus on the Web3 primitives used (DeFi, NFTs, DAOs)',
                'Highlight team experience in blockchain development'
            ]
        },
        'mlh.io': {
            'name': 'MLH Hackathon Mentor',
            'expertise': 'Major League Hacking events, beginner-friendly hackathons',
            'tips': [
                'MLH judges value learning journey over polish',
                'Document what you learned, not just what you built',
                'Engage with sponsor challenges for bonus prizes',
                'Team size matters - 2-4 is optimal'
            ]
        },
        'angelhack.com': {
            'name': 'AngelHack Accelerator Coach',
            'expertise': 'AngelHack hackathons related accelerator programs',
            'tips': [
                'Focus on market viability and business model',
                'AngelHack values founder potential',
                'Include user validation or early traction',
                'Pitch deck quality matters here'
            ]
        },
        'gitcoin.co': {
            'name': 'Gitcoin Grants Advisor',
            'expertise': 'Gitcoin grants, quadratic funding, public goods',
            'tips': [
                'Emphasize public goods and open source impact',
                'Community engagement metrics matter for QF',
                'Include clear milestones and deliverables',
                'Previous contribution history helps credibility'
            ]
        },
        'hackerone.com': {
            'name': 'Bug Bounty Specialist',
            'expertise': 'HackerOne bug bounty submissions, vulnerability reports',
            'tips': [
                'Be specific about vulnerability type (OWASP classification)',
                'Include clear reproduction steps',
                'Explain business impact in terms of risk',
                'Provide proof-of-concept but avoid destructive testing'
            ]
        },
        'immunefi.com': {
            'name': 'Web3 Security Researcher',
            'expertise': 'Smart contract auditing, DeFi security, blockchain vulnerabilities',
            'tips': [
                'Focus on fund-at-risk calculations',
                'Reference similar past vulnerabilities',
                'Include on-chain evidence and transaction hashes',
                'Explain attack vectors in detail'
            ]
        },
        'kaggle.com': {
            'name': 'Kaggle Competition Expert',
            'expertise': 'Data science competitions, ML model optimization',
            'tips': [
                'Document your approach in a clear notebook',
                'Ensemble methods often win',
                'Feature engineering is key',
                'Validate with robust cross-validation'
            ]
        }
    }
    
    def _detect_platform(self, url: str) -> Dict[str, Any]:
        """Detect which platform the user is on and return persona"""
        if not url:
            return {'name': 'General Application Assistant', 'tips': []}
        
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace('www.', '')
        
        for platform_domain, persona in self.PLATFORM_PERSONAS.items():
            if platform_domain in domain:
                return persona
        
        return {
            'name': 'General Application Assistant',
            'expertise': 'Scholarship and hackathon applications',
            'tips': [
                'Be specific about your achievements with metrics',
                'Connect your experience to the opportunity requirements',
                'Show genuine enthusiasm for the specific opportunity'
            ]
        }
    
    async def chat(self, 
                   query: str, 
                   page_context: Dict[str, Any], 
                   project_context: Optional[str] = None,
                   user_profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main chat handler V2 - Now with platform-aware coaching.
        
        TRI-FOLD KNOWLEDGE BASE:
        1. User Profile (skills, interests, background)
        2. Uploaded Assets (resume, project docs)
        3. Real-Time Page Context (current application page)
        """
        
        # V2: Detect platform and get specialized persona
        page_url = page_context.get('url', '')
        platform_persona = self._detect_platform(page_url)
        
        prompt = f"""
You are the ScholarStream Co-Pilot V2: **{platform_persona['name']}**

You are an elite AI agent with deep expertise in: {platform_persona.get('expertise', 'opportunity applications')}

=== TRI-FOLD KNOWLEDGE BASE ===

1️⃣ USER PROFILE (Identity Layer):
{json.dumps(user_profile, indent=2) if user_profile else "Not provided - ask user to complete their profile for personalized help"}

2️⃣ PROJECT CONTEXT (Document Layer):
{project_context if project_context else "No project document uploaded. Suggest user upload their resume/project README for better assistance."}

3️⃣ CURRENT PAGE CONTEXT (Environment Layer):
- Platform: {platform_persona['name']}
- URL: {page_url}
- Page Title: {page_context.get('title', 'Unknown')}
- Visible Content (Truncated): {page_context.get('content', '')[:40000]}

=== PLATFORM-SPECIFIC TIPS ===
{chr(10).join(f"• {tip}" for tip in platform_persona.get('tips', []))}

=== USER QUERY ===
"{query}"

=== YOUR TASK ===
1. **Analyze** the user's query in context of the current page and their profile
2. **Determine Intent**:
   - Q&A: Answer questions about the opportunity/platform
   - DRAFTING: Write essays, cover letters, short answers using their profile
   - COACHING: Provide strategic advice for winning this specific type of opportunity
   - FILLING: Generate field auto-fill actions when explicitly requested
3. **Personalize**: Reference specific skills/projects from their profile when relevant
4. **Platform Expertise**: Apply your specialized knowledge of this platform's judging criteria

=== OUTPUT FORMAT (JSON ONLY) ===
{{
  "thought_process": "Brief internal reasoning",
  "message": "Your response to the user. Be helpful, specific, and actionable.",
  "action": {{
    "type": "fill_field",
    "selector": "css_selector",
    "value": "content"
  }} OR null,
  "coaching_tips": ["Optional strategic tips specific to this platform"]
}}
"""
        try:
            result = await ai_service.generate_content_async(prompt)
            
            if not result:
                raise ValueError("Empty response from AI Service")

            # Parse JSON with robust cleanup
            text = result.strip()
            if text.startswith('```json'):
                text = text[7:]
            if text.startswith('```'):
                text = text[3:]
            if text.endswith('```'):
                text = text[:-3]
            text = text.strip()
            
            response_data = json.loads(text)
            
            return {
                "message": response_data.get("message", "I processed that for you."),
                "action": response_data.get("action"),
                "coaching_tips": response_data.get("coaching_tips", []),
                "platform": platform_persona['name']
            }
            
        except json.JSONDecodeError:
            logger.error("Copilot JSON Parse Error", raw_result=result[:200] if result else "None")
            return {
                "message": f"I understood your request. {result[:500] if result else 'Please try again.'}",
                "action": None,
                "platform": platform_persona['name']
            }
        except Exception as e:
            logger.error("Copilot chat failed", error=str(e))
            return {
                "message": "I'm having trouble connecting to my brain right now. Please try again in a moment.",
                "action": None,
                "platform": platform_persona['name']
            }

    async def generate_field_content(
        self, 
        target_field: Dict[str, Any], 
        user_profile: Dict[str, Any], 
        instruction: Optional[str] = None,
        page_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Sparkle V2 / Focus Fill Handler - Now with platform awareness.
        Generates content for a SINGLE specific field with high precision.
        """
        platform_persona = self._detect_platform(page_url or '')
        
        prompt = f"""
You are the "{platform_persona['name']}" Sparkle Engine for ScholarStream.
A student needs help filling a specific form field on {platform_persona['name']}.

USER PROFILE:
{json.dumps(user_profile, indent=2)}

TARGET FIELD INFO:
- Label: {target_field.get('label')}
- Name/ID: {target_field.get('name')} / {target_field.get('id')}
- Placeholder: {target_field.get('placeholder')}
- Type: {target_field.get('type')}
- Surrounding Text: {target_field.get('surroundingText', '')}

USER INSTRUCTION: {instruction or "Fill this based on my profile."}

PLATFORM CONTEXT: This is for {platform_persona['name']}. Apply these tips:
{chr(10).join(f"• {tip}" for tip in platform_persona.get('tips', [])[:2])}

TASK:
1. Analyze what this field needs based on label, placeholder, and surrounding context
2. Draft perfect content using the user's profile data
3. Tailor the tone and content to what {platform_persona['name']} judges/reviewers expect

OUTPUT JSON:
{{
  "content": "The actual text to fill in the box (ready to paste)",
  "reasoning": "Brief explanation of your approach"
}}
"""
        try:
            result = await ai_service.generate_content_async(prompt)
            
            # Simple cleanup for JSON
            cleaned = result.strip()
            if cleaned.startswith('```json'): cleaned = cleaned[7:]
            if cleaned.startswith('```'): cleaned = cleaned[3:]
            if cleaned.endswith('```'): cleaned = cleaned[:-3]
            
            data = json.loads(cleaned.strip())
            return data
        except Exception as e:
            logger.error("Sparkle generation failed", error=str(e))
            return {
                "content": "",
                "reasoning": "I had a brain freeze. Please type manually for now."
            }


copilot_service = CopilotService()
