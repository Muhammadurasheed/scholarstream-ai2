import { useState, useRef, useEffect } from 'react';
import { MessageCircle, X, Send, Sparkles, Loader2, Maximize2, Minimize2, Bookmark, ExternalLink, ArrowRight, ChevronDown, ChevronUp, Brain } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { useAuth } from '@/contexts/AuthContext';
import { toast } from 'sonner';
import { Scholarship } from '@/types/scholarship';
import { normalizeApplyUrl } from '@/utils/scholarshipUtils';
import { useNavigate } from 'react-router-dom';
import { cn } from '@/lib/utils';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface Action {
  type: 'navigate' | 'save' | 'apply' | 'filter' | 'external_link';
  label: string;
  data: any;
}

interface Message {
  role: 'user' | 'assistant';
  content: string;
  thinkingProcess?: string;  // V2: Separate thinking process for collapsible display
  opportunities?: Scholarship[];
  actions?: Action[];
  suggestions?: string[];
}

// V2: Collapsible Thinking Process Component
const ThinkingProcessSection = ({ content }: { content: string }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  
  if (!content) return null;
  
  return (
    <div className="mb-3 rounded-lg border border-primary/20 bg-primary/5 overflow-hidden">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between px-3 py-2 text-xs font-medium text-primary hover:bg-primary/10 transition-colors"
      >
        <div className="flex items-center gap-2">
          <Brain className="h-3.5 w-3.5" />
          <span>Search Analysis</span>
        </div>
        {isExpanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
      </button>
      
      {isExpanded && (
        <div className="px-3 pb-3 text-xs text-muted-foreground">
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={{
              p: ({ node, ...props }) => <p className="my-1" {...props} />,
              strong: ({ node, ...props }) => <strong className="text-foreground font-semibold" {...props} />,
              ul: ({ node, ...props }) => <ul className="list-disc list-inside my-1" {...props} />,
              li: ({ node, ...props }) => <li className="ml-2" {...props} />,
            }}
          >
            {content}
          </ReactMarkdown>
        </div>
      )}
    </div>
  );
};

export const FloatingChatAssistant = () => {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [isOpen, setIsOpen] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([
    {
      role: 'assistant',
      content: "Hi! I'm your ScholarStream AI assistant. I can help you find scholarships, hackathons, bounties, and more. What are you looking for?",
      suggestions: [
        'Find urgent opportunities',
        'Scholarships for my major',
        'High-value opportunities',
      ],
    },
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Auto-grow textarea as user types
  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value);

    // Auto-grow logic
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 200)}px`;
    }
  };

  // Strip HTML tags from AI responses
  const stripHtml = (text: string): string => {
    if (!text) return '';
    // Remove HTML tags
    return text
      .replace(/<[^>]*>/g, '')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&amp;/g, '&')
      .replace(/&quot;/g, '"');
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const handleSend = async (message?: string) => {
    const messageText = message || input.trim();
    if (!messageText || !user) return;

    const userMessage: Message = { role: 'user', content: messageText };
    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8081';

      const response = await fetch(`${API_BASE_URL}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: user.uid,
          message: messageText,
          context: {
            current_page: window.location.pathname,
          },
        }),
      });

      if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
      }

      const data = await response.json();

      // Build structured response with actions and suggestions
      // V2: Handle separate thinking_process from backend
      const assistantMessage: Message = {
        role: 'assistant',
        content: data.message,
        thinkingProcess: data.thinking_process || '',  // V2: Separate field
        opportunities: data.opportunities || [],
        actions: data.actions || buildDefaultActions(data.opportunities),
        suggestions: data.suggestions || buildDefaultSuggestions(messageText),
      };

      setMessages(prev => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Chat error:', error);

      const fallbackMessage: Message = {
        role: 'assistant',
        content: "I'm having trouble connecting right now. Try browsing the dashboard for opportunities, or ask me again in a moment.",
        suggestions: [
          'Try again',
          'Browse dashboard',
          'View saved opportunities',
        ],
      };
      setMessages(prev => [...prev, fallbackMessage]);
      toast.error('Connection issue. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  const buildDefaultActions = (opportunities?: Scholarship[]): Action[] => {
    if (!opportunities || opportunities.length === 0) return [];

    const actions: Action[] = [];

    if (opportunities.length > 0) {
      actions.push({
        type: 'navigate',
        label: '🎯 View Top Match',
        data: { path: `/opportunity/${opportunities[0].id}` }
      });
    }

    if (opportunities.length >= 2) {
      actions.push({
        type: 'save',
        label: '💾 Save All',
        data: { opportunity_ids: opportunities.map(o => o.id) }
      });
    }

    return actions;
  };

  const buildDefaultSuggestions = (lastQuery: string): string[] => {
    const lowered = lastQuery.toLowerCase();

    if (lowered.includes('scholarship')) {
      return ['Show more scholarships', 'Filter by deadline', 'Find similar opportunities'];
    }
    if (lowered.includes('hackathon')) {
      return ['More hackathons', 'Filter by prize amount', 'Team-based events'];
    }
    if (lowered.includes('urgent') || lowered.includes('deadline')) {
      return ['This week only', 'Next 30 days', 'Show all deadlines'];
    }

    return ['Refine search', 'Show more results', 'Different category'];
  };

  const handleAction = async (action: Action) => {
    switch (action.type) {
      case 'navigate':
        navigate(action.data.path);
        setIsOpen(false);
        break;

      case 'save':
        toast.success(`Saved ${action.data.opportunity_ids?.length || 0} opportunities!`);
        break;

      case 'apply':
        navigate(`/apply/${action.data.opportunity_id}`);
        setIsOpen(false);
        break;

      case 'external_link':
        window.open(action.data.url, '_blank');
        break;

      case 'filter':
        // Navigate to dashboard with filter
        navigate('/dashboard', { state: { filter: action.data.filter } });
        setIsOpen(false);
        break;
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  if (!isOpen) {
    return (
      <Button
        onClick={() => setIsOpen(true)}
        className="fixed bottom-6 right-6 h-16 w-16 rounded-full shadow-lg hover:scale-110 transition-transform bg-gradient-to-r from-primary to-primary/80 z-50"
        size="icon"
      >
        <MessageCircle className="h-6 w-6" />
        <Sparkles className="h-4 w-4 absolute -top-1 -right-1 text-yellow-400" />
      </Button>
    );
  }

  return (
    <Card className={cn(
      'fixed flex flex-col shadow-2xl border-primary/20 animate-in slide-in-from-bottom-4 z-50 transition-all duration-300',
      isFullscreen
        ? 'inset-4 w-auto h-auto max-w-full max-h-full'
        : 'bottom-6 right-6 w-[400px] md:w-[450px] h-[600px] max-h-[80vh]'
    )}>
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b bg-gradient-to-r from-primary/10 to-primary/5">
        <div className="flex items-center gap-2">
          <div className="h-10 w-10 rounded-full bg-gradient-to-r from-primary to-primary/80 flex items-center justify-center">
            <Sparkles className="h-5 w-5 text-primary-foreground" />
          </div>
          <div>
            <h3 className="font-semibold text-foreground">ScholarStream AI</h3>
            <p className="text-xs text-muted-foreground">Your opportunity finder</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setIsFullscreen(!isFullscreen)}
            className="text-muted-foreground hover:text-foreground"
          >
            {isFullscreen ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
          </Button>
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setIsOpen(false)}
            className="text-muted-foreground hover:text-foreground"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Messages */}
      <ScrollArea className="flex-1 p-4" ref={scrollRef}>
        <div className="space-y-4">
          {messages.map((message, index) => (
            <div
              key={index}
              className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              <div
                className={cn(
                  'max-w-[85%] rounded-lg p-3',
                  message.role === 'user'
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-muted text-foreground'
                )}
              >
                {/* V2: Collapsible Thinking Process Section */}
                {message.thinkingProcess && (
                  <ThinkingProcessSection content={message.thinkingProcess} />
                )}

                {/* ENHANCED: Markdown rendering for main response */}
                <div className="prose prose-sm dark:prose-invert max-w-none">
                  <ReactMarkdown
                    remarkPlugins={[remarkGfm]}
                    components={{
                      h2: ({ node, ...props }) => (
                        <h2 className="text-base font-bold mt-3 mb-2 flex items-center gap-2" {...props} />
                      ),
                      ul: ({ node, ...props }) => (
                        <ul className="list-disc list-inside space-y-1 my-2" {...props} />
                      ),
                      li: ({ node, ...props }) => (
                        <li className="text-sm" {...props} />
                      ),
                      strong: ({ node, ...props }) => (
                        <strong className="font-semibold text-foreground" {...props} />
                      ),
                      blockquote: ({ node, ...props }) => (
                        <blockquote className="border-l-4 border-primary pl-3 italic my-2" {...props} />
                      ),
                      hr: ({ node, ...props }) => (
                        <hr className="my-3 border-border" {...props} />
                      ),
                    }}
                  >
                    {stripHtml(message.content)}
                  </ReactMarkdown>
                </div>

                {/* Opportunity Cards */}
                {message.opportunities && message.opportunities.length > 0 && (
                  <div className="mt-3 space-y-2">
                    {message.opportunities.slice(0, 3).map((opp) => (
                      <Card key={opp.id} className="p-3 bg-background">
                        <div className="flex items-start justify-between gap-2">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <Badge variant="secondary" className="text-xs">
                                {opp.source_type}
                              </Badge>
                              {opp.priority_level === 'urgent' && (
                                <Badge variant="destructive" className="text-xs">
                                  Urgent
                                </Badge>
                              )}
                              {opp.match_score && (
                                <Badge variant="outline" className="text-xs">
                                  {opp.match_score}% Match
                                </Badge>
                              )}
                            </div>
                            <h4 className="font-semibold text-sm truncate">{opp.name}</h4>
                            <p className="text-xs text-muted-foreground">{opp.organization}</p>
                            <div className="flex items-center gap-2 mt-2">
                              <span className="text-xs font-semibold text-success">
                                {stripHtml(opp.amount_display || '')}
                              </span>
                              {opp.deadline && (
                                <span className="text-xs text-muted-foreground">
                                  Due: {new Date(opp.deadline).toLocaleDateString()}
                                </span>
                              )}
                            </div>
                          </div>
                        </div>
                        <div className="flex gap-2 mt-2">
                          <Button
                            size="sm"
                            variant="outline"
                            className="flex-1 h-7 text-xs"
                            onClick={() => navigate(`/opportunity/${opp.id}`)}
                          >
                            <ExternalLink className="h-3 w-3 mr-1" />
                            View
                          </Button>
                          <Button
                            size="sm"
                            className="flex-1 h-7 text-xs"
                            onClick={() =>
                              opp.source_url
                                ? window.open(normalizeApplyUrl(opp.source_url), '_blank', 'noopener,noreferrer')
                                : navigate(`/opportunity/${opp.id}`)
                            }
                          >
                            Apply
                          </Button>
                        </div>
                      </Card>
                    ))}
                  </div>
                )}

                {/* Action Buttons */}
                {message.actions && message.actions.length > 0 && (
                  <div className="mt-3 flex flex-wrap gap-2">
                    {message.actions.map((action, i) => (
                      <Button
                        key={i}
                        variant="secondary"
                        size="sm"
                        className="text-xs h-8"
                        onClick={() => handleAction(action)}
                      >
                        {action.label}
                      </Button>
                    ))}
                  </div>
                )}

                {/* Suggestion Chips */}
                {message.suggestions && message.suggestions.length > 0 && (
                  <div className="mt-3">
                    <p className="text-xs text-muted-foreground mb-2">Try asking:</p>
                    <div className="flex flex-wrap gap-2">
                      {message.suggestions.map((suggestion, i) => (
                        <button
                          key={i}
                          className="text-xs px-3 py-1.5 rounded-full border border-border hover:bg-secondary transition-colors"
                          onClick={() => handleSend(suggestion)}
                        >
                          {suggestion}
                          <ArrowRight className="h-3 w-3 ml-1 inline" />
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          ))}

          {/* ENHANCED: Real-time search indicator */}
          {isLoading && (
            <div className="flex justify-start">
              <div className="bg-muted rounded-lg p-3 flex flex-col gap-2">
                <div className="flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin text-primary" />
                  <span className="text-xs font-semibold">Searching opportunities...</span>
                </div>
                <div className="text-xs text-muted-foreground space-y-1">
                  <div className="flex items-center gap-2">
                    <div className="h-1.5 w-1.5 rounded-full bg-primary animate-pulse" />
                    <span>Scanning database</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="h-1.5 w-1.5 rounded-full bg-primary animate-pulse" style={{ animationDelay: '200ms' }} />
                    <span>Filtering by location & deadline</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="h-1.5 w-1.5 rounded-full bg-primary animate-pulse" style={{ animationDelay: '400ms' }} />
                    <span>Ranking by match score</span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </ScrollArea>

      {/* Input */}
      <div className="p-4 border-t">
        <div className="flex gap-2">
          <Textarea
            ref={textareaRef}
            placeholder="Ask anything... (Shift+Enter for new line)"
            value={input}
            onChange={handleInputChange}
            onKeyDown={handleKeyPress}
            className="min-h-[44px] max-h-[200px] resize-none transition-all duration-200"
            disabled={isLoading}
            rows={1}
            style={{ overflow: 'hidden' }}
          />
          <Button
            size="icon"
            onClick={() => handleSend()}
            disabled={!input.trim() || isLoading}
            className="shrink-0"
          >
            <Send className="h-4 w-4" />
          </Button>
        </div>
        <p className="text-xs text-muted-foreground mt-2">
          AI can make mistakes. Verify important information.
        </p>
      </div>
    </Card>
  );
};