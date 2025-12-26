console.log("ScholarStream Content Script Loaded");

import { getPageContext } from '../utils/domScanner';

// ===== REAL AUTH: Extract token from ScholarStream web app =====
// ===== REAL AUTH: Extract token from ScholarStream web app =====
// Note: Firebase usually stores tokens in IndexedDB, but some apps mirror to localStorage.
// If the app uses standard Firebase SDK, we might need to look for `firebase:authUser:${apiKey}:[AppName]`
// For now, let's try to capture ANY key that looks like a token or the specific custom key.
if (window.location.host.includes('localhost') || window.location.host.includes('scholarstream')) {
    const extractAndSendToken = () => {
        // 1. Try custom key
        let token = localStorage.getItem('scholarstream_auth_token');

        // 2. Try standard Firebase key pattern if custom not found
        if (!token) {
            Object.keys(localStorage).forEach(key => {
                if (key.includes('firebase:authUser')) {
                    try {
                        const user = JSON.parse(localStorage.getItem(key) || '{}');
                        if (user.stsTokenManager && user.stsTokenManager.accessToken) {
                            token = user.stsTokenManager.accessToken;
                        }
                    } catch (e) {
                        // ignore parse errors
                    }
                }
            });
        }

        if (token) {
            chrome.storage.local.set({ authToken: token }, () => {
                // Only log if it's a new token to avoid noise
                chrome.storage.local.get(['lastLoggedToken'], (result) => {
                    if (result.lastLoggedToken !== token) {
                        console.log('🔑 [EXT] Real Firebase token captured!');
                        chrome.storage.local.set({ lastLoggedToken: token });
                    }
                });
            });
        }
    };

    extractAndSendToken();
    // Poll more frequently to catch login events immediately
    setInterval(extractAndSendToken, 2000);
}
// ===== END REAL AUTH =====

// ===== MIND-BLOWING UX: Sparkle Focus Engine =====
class FocusEngine {
    private activeElement: HTMLElement | null = null;
    private sparkleBtn: HTMLDivElement;
    private tooltip: HTMLDivElement;
    private thoughtBubble: HTMLDivElement; // NEW: Mental model display
    private isStreaming = false;
    private isDragging = false;
    private dragOffset = { x: 0, y: 0 };
    private sparkleHidden = false;

    constructor() {
        this.sparkleBtn = this.createSparkleButton();
        this.tooltip = this.createTooltip();
        this.thoughtBubble = this.createThoughtBubble(); // Init
        this.initListeners();
    }

    private createSparkleButton(): HTMLDivElement {
        // Container for sparkle + close button
        const container = document.createElement('div');
        container.id = 'ss-sparkle-container';
        container.style.cssText = `
            position: absolute;
            display: none;
            z-index: 2147483647;
            cursor: grab;
        `;

        // Main Sparkle Button
        const btn = document.createElement('div');
        btn.id = 'ss-sparkle-trigger';
        btn.style.cssText = `
            width: 32px;
            height: 32px;
            background: linear-gradient(135deg, #FF6B6B, #4ECDC4);
            border-radius: 50%;
            cursor: pointer;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            display: flex;
            align-items: center;
            justify-content: center;
            transition: transform 0.2s cubic-bezier(0.34, 1.56, 0.64, 1);
            animation: ss-pulse 2s infinite;
        `;
        btn.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L15.09 8.26L22 9.27L17 14.14L18.18 21.02L12 17.77L5.82 21.02L7 14.14L2 9.27L8.91 8.26L12 2Z"></path></svg>`;

        // Close Button (X)
        const closeBtn = document.createElement('div');
        closeBtn.id = 'ss-sparkle-close';
        closeBtn.style.cssText = `
            position: absolute;
            top: -8px;
            right: -8px;
            width: 18px;
            height: 18px;
            background: #ef4444;
            border-radius: 50%;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 12px;
            font-weight: bold;
            color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            opacity: 0;
            transition: opacity 0.2s;
        `;
        closeBtn.innerHTML = '×';

        container.appendChild(btn);
        container.appendChild(closeBtn);

        // Show close on hover
        container.onmouseenter = () => {
            closeBtn.style.opacity = '1';
            if (!this.isDragging) btn.style.transform = 'scale(1.1)';
        };
        container.onmouseleave = () => {
            closeBtn.style.opacity = '0';
            btn.style.transform = 'scale(1)';
        };

        // Inject animation styles
        const style = document.createElement('style');
        style.textContent = `
            @keyframes ss-pulse { 0% { box-shadow: 0 0 0 0 rgba(78, 205, 196, 0.7); } 70% { box-shadow: 0 0 0 10px rgba(78, 205, 196, 0); } 100% { box-shadow: 0 0 0 0 rgba(78, 205, 196, 0); } }
            @keyframes ss-typewriter { from { width: 0; } to { width: 100%; } }
            @keyframes ss-fade-in-up { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
            #ss-sparkle-container.dragging { cursor: grabbing !important; }
        `;
        document.head.appendChild(style);
        document.body.appendChild(container);

        // Click to trigger autofill
        btn.onclick = (e) => {
            if (this.isDragging) return;
            e.preventDefault();
            e.stopPropagation();
            this.handleSparkleClick();
        };

        // Close button hides sparkle for this session
        closeBtn.onclick = (e) => {
            e.preventDefault();
            e.stopPropagation();
            this.sparkleHidden = true;
            this.hideSparkle();
        };

        // Drag functionality
        container.onmousedown = (e) => {
            if ((e.target as HTMLElement).id === 'ss-sparkle-close') return;
            this.isDragging = true;
            container.classList.add('dragging');
            const rect = container.getBoundingClientRect();
            this.dragOffset = {
                x: e.clientX - rect.left,
                y: e.clientY - rect.top
            };
        };

        document.addEventListener('mousemove', (e) => {
            if (!this.isDragging) return;
            container.style.left = `${e.clientX - this.dragOffset.x + window.scrollX}px`;
            container.style.top = `${e.clientY - this.dragOffset.y + window.scrollY}px`;
        });

        document.addEventListener('mouseup', () => {
            if (this.isDragging) {
                this.isDragging = false;
                container.classList.remove('dragging');
            }
        });

        return container as HTMLDivElement;
    }

    private createTooltip() {
        const div = document.createElement('div');
        div.style.cssText = `
            position: absolute;
            display: none;
            background: #1e293b;
            color: #fff;
            padding: 8px 12px;
            border-radius: 8px;
            font-size: 12px;
            font-family: sans-serif;
            z-index: 2147483647;
            pointer-events: none;
            white-space: nowrap;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        `;
        div.innerText = "✨ Auto-Fill with ScholarStream";
        document.body.appendChild(div);
        return div;
    }

    private createThoughtBubble() {
        const div = document.createElement('div');
        div.style.cssText = `
            position: absolute;
            display: none;
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            color: #e2e8f0;
            padding: 12px 16px;
            border-radius: 12px;
            border: 1px solid #334155;
            font-size: 13px;
            font-family: 'Inter', system-ui, sans-serif;
            line-height: 1.4;
            max-width: 320px;
            z-index: 2147483647;
            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5);
            pointer-events: none;
            opacity: 0;
            transform: translateY(10px);
            transition: opacity 0.3s, transform 0.3s;
        `;
        document.body.appendChild(div);
        return div;
    }

    // ... initListeners ...
    private initListeners() {
        document.addEventListener('focusin', (e) => this.handleFocus(e), true);
        document.addEventListener('scroll', () => this.updatePosition(), true);
        window.addEventListener('resize', () => this.updatePosition());
    }

    private handleFocus(e: FocusEvent) {
        const target = e.target as HTMLElement;
        if (!target) return;

        // Ignore if not an input-like element
        if (!['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName) && !target.isContentEditable) {
            this.hideSparkle();
            return;
        }

        const input = target as HTMLInputElement;
        // Ignore file inputs, hidden inputs, submit buttons
        if (input.type === 'file' || input.type === 'hidden' || input.type === 'submit' || input.type === 'image') {
            this.hideSparkle();
            return;
        }

        this.activeElement = target;
        this.showSparkle(target);
    }

    private showSparkle(target: HTMLElement) {
        if (!target || this.sparkleHidden) return;
        const rect = target.getBoundingClientRect();

        // Position inside right edge
        const top = rect.top + window.scrollY + (rect.height / 2) - 16;
        const left = rect.right + window.scrollX - 40; // 40px from right edge

        this.sparkleBtn.style.top = `${top}px`;
        this.sparkleBtn.style.left = `${left}px`;
        this.sparkleBtn.style.display = 'flex';

        // Tooltip logic
        this.tooltip.style.top = `${top - 30}px`;
        this.tooltip.style.left = `${left - 60}px`;
    }

    private hideSparkle() {
        this.sparkleBtn.style.display = 'none';
        this.tooltip.style.display = 'none';
        this.thoughtBubble.style.opacity = '0'; // Hide thought bubble too
    }

    private updatePosition() {
        if (this.activeElement && this.sparkleBtn.style.display !== 'none') {
            this.showSparkle(this.activeElement);
        }
    }

    private showReasoning(text: string, target: HTMLElement) {
        if (!text || !target) return;

        const rect = target.getBoundingClientRect();
        const top = rect.bottom + window.scrollY + 8;
        const left = rect.left + window.scrollX;

        this.thoughtBubble.innerHTML = `<span style="color: #4ECDC4; font-weight: 600;">🧠 AI Thought:</span> ${text}`;
        this.thoughtBubble.style.top = `${top}px`;
        this.thoughtBubble.style.left = `${left}px`;
        this.thoughtBubble.style.display = 'block';

        // Trigger reflow for transition
        void this.thoughtBubble.offsetWidth;

        this.thoughtBubble.style.opacity = '1';
        this.thoughtBubble.style.transform = 'translateY(0)';

        // Auto-hide after 5 seconds
        setTimeout(() => {
            this.thoughtBubble.style.opacity = '0';
            this.thoughtBubble.style.transform = 'translateY(10px)';
            setTimeout(() => {
                if (this.thoughtBubble.style.opacity === '0') {
                    this.thoughtBubble.style.display = 'none';
                }
            }, 300);
        }, 6000);
    }

    private async handleSparkleClick() {
        if (!this.activeElement || this.isStreaming) return;

        this.isStreaming = true;
        const target = this.activeElement as HTMLInputElement;

        // Animation: Spin
        this.sparkleBtn.innerHTML = `<svg class="animate-spin" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" ...><circle cx="12" cy="12" r="10" opacity="0.25"/><path d="M12 2C6.48 2 2 6.48 2 12" opacity="0.75"/></svg>`;

        try {
            // 1. Gather Context
            const fieldData = {
                id: target.id,
                name: (target as any).name,
                type: (target as any).type,
                placeholder: (target as any).placeholder || '',
                label: this.getLabel(target),
                surroundingText: "", // Keep simple or fetch real text
                pageTitle: document.title,
                pageUrl: window.location.href
            };

            // 2. Fetch Generation
            const result = await generateFieldContent(fieldData);

            // Handle new sparkle_result format from backend
            const content = result.sparkle_result?.content || result.filled_value;
            const reasoning = result.sparkle_result?.reasoning || result.reasoning;

            if (content) {
                // SHOW REASONING
                if (reasoning) {
                    this.showReasoning(reasoning, target);
                }

                await this.typewriterEffect(target, content);
            }
        } catch (e) {
            console.error("Focus Fill Failed", e);
        } finally {
            this.isStreaming = false;
            // Reset Icon
            this.sparkleBtn.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2"><path d="M12 2L15.09 8.26L22 9.27L17 14.14L18.18 21.02L12 17.77L5.82 21.02L7 14.14L2 9.27L8.91 8.26L12 2Z"></path></svg>`;
        }
    }

    private getLabel(el: HTMLElement): string {
        return (
            document.querySelector(`label[for="${el.id}"]`)?.textContent?.trim() ||
            el.closest('label')?.textContent?.trim() ||
            el.previousElementSibling?.textContent?.trim() ||
            el.parentElement?.textContent?.trim() ||
            ''
        ).slice(0, 100);
    }

    private async typewriterEffect(element: HTMLInputElement | HTMLTextAreaElement, text: string) {
        element.value = "";
        element.focus();

        // FIX: Use 'speed' variable
        const speed = Math.max(10, Math.min(50, 1000 / text.length));

        for (let i = 0; i < text.length; i++) {
            element.value += text.charAt(i);
            element.dispatchEvent(new Event('input', { bubbles: true }));
            if (element.scrollTop !== undefined) element.scrollTop = element.scrollHeight;

            // Random jitter using 'speed' as base
            await new Promise(r => setTimeout(r, speed + Math.random() * 10));
        }
        element.dispatchEvent(new Event('change', { bubbles: true }));

        // Flash green
        const originalBg = element.style.backgroundColor;
        element.style.transition = "background-color 0.5s";
        element.style.backgroundColor = "#dcfce7"; // Green-100
        setTimeout(() => element.style.backgroundColor = originalBg, 1000);
    }
}

// Initialize Focus Engine
new FocusEngine();

// API Helper for Single Field
async function generateFieldContent(targetField: any) {
    let userProfile: any = {};
    try {
        const stored = await chrome.storage.local.get(['userProfile']);
        userProfile = stored.userProfile || {};
    } catch (e) { }

    // Get Auth
    const storedToken = (await chrome.storage.local.get(['authToken'])).authToken;
    const authToken = storedToken || 'TEST_TOKEN';

    // Get Context (Doc)
    let projectContext = "";
    try {
        const stored = await chrome.storage.local.get(['projectContext']);
        projectContext = stored.projectContext || "";
    } catch (e) { }

    const response = await fetch('http://localhost:8081/api/extension/map-fields', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${authToken}`
        },
        body: JSON.stringify({
            form_fields: [], // Empty because we are targeting one
            user_profile: userProfile,
            target_field: targetField,
            project_context: projectContext, // Sent from storage
            instruction: "Fill this field based on my profile and project." // Default instruction
        })
    });

    return await response.json();
}


// --- CONNECTION & SAFETY HELPERS ---

// Safe Message Sender (prevents "Extension context invalidated")
const safeSendMessage = async (message: any) => {
    if (!chrome.runtime?.id) {
        console.warn("Extension context invalidated. Reload page to reconnect.");
        return;
    }
    try {
        return await chrome.runtime.sendMessage(message);
    } catch (e) {
        // Suppress the noisy error, it just means connection lost
        const msg = (e as any).message || "";
        if (msg.includes("Extension context invalidated") || msg.includes("receiving end does not exist")) {
            console.log("Extension disconnected (reload needed).");
        } else {
            console.error("Message send failed:", e);
        }
    }
};

// Listen for context requests and Auto-Fill commands
chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    // 1. Validate Context first
    if (!chrome.runtime?.id) return;

    try {
        if (message.type === 'GET_PAGE_CONTEXT') {
            const context = getPageContext();
            console.log("Creating context:", context.title);
            sendResponse(context);
        }

        if (message.type === 'AUTO_FILL_REQUEST') {
            console.log("Agentic Auto-Fill Triggered");
            handleAutoFill(message.projectContext).then(result => {
                try {
                    sendResponse(result);
                } catch (e) { /* Context likely lost during long op */ }
            });
            return true; // Keep channel open for async response
        }
    } catch (e) {
        console.error("Content Script Error:", e);
    }
});

async function handleAutoFill(projectContext?: string) {
    // 1. Scrape Form Fields
    const inputs = Array.from(document.querySelectorAll('input, select, textarea'));
    const formFields = inputs.map((el: any) => ({
        id: el.id,
        name: el.name,
        type: el.type || el.tagName.toLowerCase(),
        placeholder: el.placeholder,

        label: (
            document.querySelector(`label[for="${el.id}"]`)?.textContent?.trim() ||
            el.closest('label')?.textContent?.trim() ||
            el.previousElementSibling?.textContent?.trim() ||
            el.parentElement?.textContent?.trim() ||
            ''
        ).slice(0, 100), // Limit label length
        selector: uniqueSelector(el)
    })).filter(f => f.type !== 'hidden' && f.type !== 'submit' && f.type !== 'file');

    if (formFields.length === 0) return { success: false, message: "No fields found" };

    // 2. Get User Profile from chrome.storage
    let userProfile: any = {};
    try {
        const stored = await chrome.storage.local.get(['userProfile']);
        userProfile = stored.userProfile || {};
    } catch (e) {
        console.log("No stored profile, using empty");
    }

    // 3. Call Backend API
    try {
        // Get auth token from storage - fall back to TEST_TOKEN for dev if not present
        const storedToken = (await chrome.storage.local.get(['authToken'])).authToken;
        // Use real token if available
        const authToken = storedToken;

        if (!authToken) {
            return { success: false, message: "Please sign in securely through the extension first." };
        }

        console.log(`🔑 [EXT] Using secure token for API call`);
        const response = await fetch('http://localhost:8081/api/extension/map-fields', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${authToken}`
            },
            body: JSON.stringify({
                form_fields: formFields,
                user_profile: userProfile,
                project_context: projectContext
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Backend error: ${response.status} - ${errorText}`);
        }

        const data = await response.json();
        const fieldMappings = data.field_mappings || {};

        // 4. Apply Mappings to DOM
        let filledCount = 0;
        for (const [selector, value] of Object.entries(fieldMappings)) {
            const el = document.querySelector(selector) as HTMLInputElement;
            if (el && value) {
                if (el.type === 'file') continue; // Skip files

                el.value = String(value);
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
                filledCount++;
                el.style.border = "2px solid #22c55e";
                el.style.backgroundColor = "#f0fdf4";
            }
        }

        return { success: true, filled: filledCount };

    } catch (error) {
        console.error("Auto-Fill Failed:", error);
        return { success: false, error: String(error) };
    }
}

// Helper to generate unique CSS selector
function uniqueSelector(el: Element): string {
    if (el.id) return `#${el.id}`;
    if ((el as any).name) return `[name="${(el as any).name}"]`;
    return el.tagName.toLowerCase();
}

// ===== SIDE PANEL TRIGGER (PULSE ICON) =====
// Only show on relevant pages
if (document.body.innerText.toLowerCase().includes('scholarship') ||
    document.body.innerText.toLowerCase().includes('hackathon') ||
    document.body.innerText.toLowerCase().includes('grant') ||
    window.location.hostname.includes('devpost') ||
    window.location.hostname.includes('kaleidico')) {

    const icon = document.createElement('div');
    icon.id = 'scholarstream-pulse-icon';
    icon.style.cssText = `
      position: fixed;
      bottom: 20px;
      right: 20px;
      width: 50px;
      height: 50px;
      background: linear-gradient(135deg, #3b82f6, #8b5cf6);
      border-radius: 50%;
      box-shadow: 0 4px 15px rgba(0,0,0,0.3);
      z-index: 9999;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      font-family: sans-serif;
      font-weight: bold;
      transition: transform 0.2s;
    `;
    icon.innerText = "SS";

    icon.onclick = () => {
        console.log("Pulse Clicked - Requesting Side Panel Open");
        safeSendMessage({ type: 'OPEN_SIDE_PANEL' });
    };

    document.body.appendChild(icon);
}
// ===== END SIDE PANEL TRIGGER =====
