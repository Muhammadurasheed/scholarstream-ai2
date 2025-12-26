import React, { useState, useRef, useEffect } from 'react';
import { Send, Upload, Sparkles, User, Bot, Mic, MicOff, FileText, LogIn, AlertCircle } from 'lucide-react';
import { signInWithEmailAndPassword } from 'firebase/auth';
import { doc, getDoc } from 'firebase/firestore';
import { auth, db } from '../utils/firebase';

interface Message {
    id: string;
    role: 'user' | 'assistant';
    text: string;
}

export default function App() {
    const [messages, setMessages] = useState<Message[]>([
        { id: '1', role: 'assistant', text: 'Hello! I am your ScholarStream Co-Pilot. I can help you apply for this opportunity. Upload your project doc or ask me anything!' }
    ]);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const [isListening, setIsListening] = useState(false);
    const [projectContext, setProjectContext] = useState<string | null>(null);
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    // Auth State
    const [authToken, setAuthToken] = useState<string | null>(null);
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [authError, setAuthError] = useState('');
    const [authLoading, setAuthLoading] = useState(false);

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    };

    useEffect(scrollToBottom, [messages]);

    // Check for existing token on mount
    useEffect(() => {
        chrome.storage.local.get(['authToken'], (result) => {
            if (result.authToken) {
                setAuthToken(result.authToken);
            }
        });

        // Listen for token changes (from sync or other sources)
        const listener = (changes: any, area: string) => {
            if (area === 'local' && changes.authToken) {
                setAuthToken(changes.authToken.newValue);
            }
        };
        chrome.storage.onChanged.addListener(listener);
        return () => chrome.storage.onChanged.removeListener(listener);
    }, []);

    // Handle Login Logic
    const handleLogin = async (e: React.FormEvent) => {
        e.preventDefault();
        setAuthError('');
        setAuthLoading(true);

        try {
            const userCredential = await signInWithEmailAndPassword(auth, email, password);
            const user = userCredential.user;
            const token = await user.getIdToken();

            // 1. Sync Token
            await chrome.storage.local.set({ authToken: token });
            setAuthToken(token);

            // 2. Fetch & Sync User Profile (Knowledge Base)
            try {
                const userDoc = await getDoc(doc(db, 'users', user.uid));
                if (userDoc.exists()) {
                    const profileData = userDoc.data();
                    await chrome.storage.local.set({ userProfile: profileData });
                    console.log("[EXT] Profile Synced:", profileData);
                }
            } catch (profileErr) {
                console.error("[EXT] Failed to sync profile:", profileErr);
                // Non-fatal, but good to know
            }

        } catch (error: any) {
            console.error("Login Failed:", error);
            setAuthError(error.message || "Invalid credentials");
        } finally {
            setAuthLoading(false);
        }
    };

    // Voice Handler (Web Speech API)
    const toggleVoice = () => {
        if (isListening) {
            // Stop logic handled by onend
            return;
        }

        const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
        if (!SpeechRecognition) {
            alert("Voice input is not supported in this browser.");
            return;
        }

        const recognition = new SpeechRecognition();
        recognition.lang = 'en-US';
        recognition.interimResults = false;
        recognition.maxAlternatives = 1;

        recognition.onstart = () => setIsListening(true);

        recognition.onresult = (event: any) => {
            const transcript = event.results[0][0].transcript;
            setInput(prev => prev + (prev ? ' ' : '') + transcript);
        };

        recognition.onend = () => setIsListening(false);
        recognition.onerror = (event: any) => {
            console.error("Speech error", event.error);
            setIsListening(false);
        };

        recognition.start();
    };

    // File Upload Handler
    const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;

        // Simple text reading for MVP (Supports .md, .txt, .json)
        // For PDF/Docx, we would send to backend for parsing, but let's do text for now as "Project Context"
        const reader = new FileReader();
        reader.onload = async (e) => {
            const text = e.target?.result as string;
            setProjectContext(text);

            // Persist Context
            chrome.storage.local.set({ projectContext: text });

            setMessages(prev => [...prev, {
                id: Date.now().toString(),
                role: 'assistant',
                text: `✅ I've read "${file.name}". I'll use this context for your answers.`
            }]);
        };
        reader.readAsText(file);
    };

    const handleSend = async () => {
        if (!input.trim()) return;

        const userMsg: Message = { id: Date.now().toString(), role: 'user', text: input };
        setMessages(prev => [...prev, userMsg]);
        setInput('');
        setLoading(true);

        try {
            // 1. Get Page Context from Content Script
            // We use a safe fallback if context extraction fails
            let context = { title: 'Unknown', url: '', content: '', forms: [] };

            try {
                const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
                if (tab?.id) {
                    // Set a timeout for the context request
                    // Using a promise wrapper for timeout handling if needed, but chrome.tabs.sendMessage usually fails fast if no listener
                    context = await chrome.tabs.sendMessage(tab.id, { type: 'GET_PAGE_CONTEXT' });
                }
            } catch (e) {
                console.warn("Could not get page context:", e);
                // Non-fatal, we continue with empty context
            }

            // 2. Call Backend API
            const response = await fetch('http://localhost:8081/api/extension/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${authToken || 'TEST_TOKEN'}`
                },
                body: JSON.stringify({
                    query: userMsg.text,
                    page_context: context,
                    project_context: projectContext // Send uploaded context
                })
            });

            if (!response.ok) {
                const errText = await response.text();
                throw new Error(`Server Error: ${response.status} - ${errText}`);
            }

            const data = await response.json();
            const aiResponse = data.data; // { message, action }

            const aiMsg: Message = {
                id: (Date.now() + 1).toString(),
                role: 'assistant',
                text: aiResponse.message || "I processed that, but have nothing to say."
            };
            setMessages(prev => [...prev, aiMsg]);

            // Handle Actions (Auto-fill)
            if (aiResponse.action && aiResponse.action.type === 'fill_field') {
                const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
                if (tab?.id) {
                    chrome.tabs.sendMessage(tab.id, {
                        type: 'FILL_FIELD',
                        selector: aiResponse.action.selector,
                        value: aiResponse.action.value
                    });
                }
            }

        } catch (error) {
            console.error("Chat Error:", error);
            setMessages(prev => [...prev, {
                id: Date.now().toString(),
                role: 'assistant',
                text: "Sorry, I couldn't reach the server. Is the backend running at localhost:8081?"
            }]);
        } finally {
            setLoading(false);
        }
    };

    const handleAutoFill = async () => {
        setLoading(true);
        try {
            const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
            if (tab?.id) {
                const response = await chrome.tabs.sendMessage(tab.id, {
                    type: 'AUTO_FILL_REQUEST',
                    projectContext: projectContext || undefined // Send the uploaded document content
                });

                // Add system message about result
                setMessages(prev => [...prev, {
                    id: Date.now().toString(),
                    role: 'assistant',
                    text: response.success
                        ? `✨ Magic! Auto-filled ${response.filled} fields based on your profile.`
                        : `❌ Auto-fill failed: ${response.message || response.error}`
                }]);
            }
        } catch (e) {
            setMessages(prev => [...prev, {
                id: Date.now().toString(),
                role: 'assistant',
                text: "Could not communicate with the page. Try refreshing the page."
            }]);
        } finally {
            setLoading(false);
        }
    };

    // --- LOGIN SCREEN ---
    if (!authToken) {
        return (
            <div className="flex flex-col h-screen bg-slate-950 text-slate-100 p-6 items-center justify-center">
                <div className="w-full max-w-xs space-y-6">
                    <div className="flex flex-col items-center gap-2">
                        <div className="w-12 h-12 bg-gradient-to-br from-blue-500 to-purple-600 rounded-xl flex items-center justify-center shadow-lg shadow-blue-500/20">
                            <Sparkles className="w-6 h-6 text-white" />
                        </div>
                        <h1 className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-400">
                            ScholarStream
                        </h1>
                        <p className="text-sm text-slate-400 text-center">
                            Sign in to sync your profile and unlock AI powers.
                        </p>
                    </div>

                    <form onSubmit={handleLogin} className="space-y-4">
                        <div className="space-y-2">
                            <label className="text-xs font-medium text-slate-500">Email</label>
                            <input
                                type="email"
                                value={email}
                                onChange={e => setEmail(e.target.value)}
                                className="w-full bg-slate-900 border border-slate-800 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-blue-600 transition-all outline-none"
                                placeholder="you@example.com"
                                required
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-xs font-medium text-slate-500">Password</label>
                            <input
                                type="password"
                                value={password}
                                onChange={e => setPassword(e.target.value)}
                                className="w-full bg-slate-900 border border-slate-800 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-blue-600 transition-all outline-none"
                                placeholder="••••••••"
                                required
                            />
                        </div>

                        {authError && (
                            <div className="flex items-center gap-2 text-red-400 text-xs bg-red-950/30 p-3 rounded-lg border border-red-900/50">
                                <AlertCircle className="w-4 h-4 shrink-0" />
                                <p>{authError}</p>
                            </div>
                        )}

                        <button
                            type="submit"
                            disabled={authLoading}
                            className="w-full bg-blue-600 hover:bg-blue-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-medium py-2.5 rounded-lg transition-all flex justify-center items-center gap-2"
                        >
                            {authLoading ? (
                                <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                            ) : (
                                <>
                                    <LogIn className="w-4 h-4" />
                                    Sign In
                                </>
                            )}
                        </button>
                    </form>
                </div>
            </div>
        );
    }

    // --- MAIN APP (Showing only when authenticated) ---
    return (
        <div className="flex flex-col h-screen bg-slate-900 text-slate-100">
            {/* Header with Logout option implicitly or minimal header */}
            <header className="p-4 border-b border-slate-800 flex items-center justify-between bg-slate-950">
                <div className="flex items-center gap-2">
                    <Sparkles className="w-5 h-5 text-blue-500" />
                    <h1 className="font-bold text-lg">Co-Pilot</h1>
                </div>
                <div className="flex items-center gap-2">
                    <button
                        onClick={() => {
                            chrome.storage.local.remove(['authToken', 'userProfile']);
                            setAuthToken(null);
                        }}
                        className="text-xs text-slate-500 hover:text-slate-300"
                        title="Sign Out"
                    >
                        Sign Out
                    </button>
                    <button
                        onClick={handleAutoFill}
                        disabled={loading}
                        className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-xs px-3 py-1.5 rounded-full flex items-center gap-1 transition-colors"
                    >
                        <Sparkles className="w-3 h-3" /> Auto-Fill
                    </button>
                </div>
            </header>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto p-4 space-y-4">
                {messages.map(msg => (
                    <div key={msg.id} className={`flex gap-3 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                        {msg.role === 'assistant' && (
                            <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center shrink-0">
                                <Bot className="w-5 h-5" />
                            </div>
                        )}
                        <div className={`max-w-[85%] rounded-lg p-3 text-sm ${msg.role === 'user'
                            ? 'bg-blue-600 text-white'
                            : 'bg-slate-800 text-slate-200 border border-slate-700'
                            }`}>
                            {msg.text}
                        </div>
                        {msg.role === 'user' && (
                            <div className="w-8 h-8 rounded-full bg-slate-700 flex items-center justify-center shrink-0">
                                <User className="w-5 h-5" />
                            </div>
                        )}
                    </div>
                ))}

                {/* File Context Indicator */}
                {projectContext && (
                    <div className="flex justify-center">
                        <div className="bg-slate-800 text-xs text-slate-400 px-3 py-1 rounded-full flex items-center gap-1 border border-slate-700">
                            <FileText className="w-3 h-3" /> Project Context Active
                        </div>
                    </div>
                )}

                {loading && (
                    <div className="flex gap-3">
                        <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center">
                            <Bot className="w-5 h-5 animate-pulse" />
                        </div>
                        <div className="bg-slate-800 rounded-lg p-3 border border-slate-700">
                            <div className="flex gap-1">
                                <div className="w-2 h-2 bg-slate-500 rounded-full animate-bounce" style={{ animationDelay: '0s' }} />
                                <div className="w-2 h-2 bg-slate-500 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }} />
                                <div className="w-2 h-2 bg-slate-500 rounded-full animate-bounce" style={{ animationDelay: '0.4s' }} />
                            </div>
                        </div>
                    </div>
                )}
                <div ref={messagesEndRef} />
            </div>

            {/* Input Area */}
            <div className="p-4 border-t border-slate-800 bg-slate-950">
                <div className="relative flex items-center gap-2">
                    <input
                        type="file"
                        ref={fileInputRef}
                        onChange={handleFileUpload}
                        className="hidden"
                        accept=".txt,.md,.json,.csv"
                    />
                    <button
                        onClick={() => fileInputRef.current?.click()}
                        className={`p-2 rounded-full transition-colors ${projectContext ? 'text-blue-400 bg-blue-500/10' : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800'}`}
                        title="Upload Project Context"
                    >
                        <Upload className="w-5 h-5" />
                    </button>

                    <div className="relative flex-1">
                        <input
                            type="text"
                            value={input}
                            onChange={e => setInput(e.target.value)}
                            onKeyDown={e => e.key === 'Enter' && handleSend()}
                            placeholder={isListening ? "Listening..." : "Ask Co-Pilot..."}
                            className={`w-full bg-slate-800 border-none rounded-full py-3 pl-4 pr-20 focus:ring-2 focus:ring-blue-600 text-sm ${isListening ? 'ring-2 ring-red-500 animate-pulse' : ''}`}
                        />
                        <button
                            onClick={toggleVoice}
                            className={`absolute right-10 top-1/2 -translate-y-1/2 p-1.5 rounded-full transition-colors ${isListening ? 'text-red-500 hover:text-red-400' : 'text-slate-400 hover:text-slate-200'}`}
                        >
                            {isListening ? <MicOff className="w-4 h-4" /> : <Mic className="w-4 h-4" />}
                        </button>
                        <button
                            onClick={handleSend}
                            disabled={!input.trim() || loading}
                            className="absolute right-2 top-1/2 -translate-y-1/2 p-1.5 bg-blue-600 rounded-full hover:bg-blue-500 disabled:opacity-50 transition-colors"
                        >
                            <Send className="w-4 h-4" />
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
