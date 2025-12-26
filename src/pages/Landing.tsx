import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { useNavigate } from 'react-router-dom';
import { ArrowRight, CheckCircle2, Sparkles, Trophy, User, Lock } from 'lucide-react';

const Landing = () => {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen">
      {/* Hero Section */}
      <section className="gradient-hero relative min-h-screen flex items-center justify-center overflow-hidden px-4">
        {/* Decorative background elements */}
        <div className="absolute inset-0 overflow-hidden opacity-20">
          <div className="absolute top-20 left-10 h-64 w-64 rounded-full bg-primary blur-3xl"></div>
          <div className="absolute bottom-20 right-10 h-96 w-96 rounded-full bg-primary-glow blur-3xl"></div>
        </div>

        <div className="relative z-10 max-w-5xl mx-auto text-center">
          <p className="text-primary-glow text-sm font-bold mb-4 animate-fade-in tracking-wide uppercase">
            Stop Leaving Money on the Table
          </p>
          <h1 className="text-5xl md:text-7xl font-bold mb-6 animate-slide-up text-foreground">
            Find $2.9 Billion in{' '}
            <span className="gradient-primary bg-clip-text text-transparent">
              Unclaimed Scholarships
            </span>
          </h1>
          <p className="text-xl md:text-2xl text-foreground/80 mb-12 max-w-3xl mx-auto animate-slide-up leading-relaxed">
            AI-powered platform that finds scholarships you actually qualify for,
            tracks deadlines, and guides you through every application.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center mb-8 animate-fade-in">
            <Button 
              size="lg" 
              className="text-lg px-8 py-6 shadow-elegant hover:shadow-glow transition-smooth hover:scale-105"
              onClick={() => navigate('/signup')}
            >
              Find My Scholarships
              <ArrowRight className="ml-2 h-5 w-5" />
            </Button>
            <Button 
              size="lg" 
              variant="outline" 
              className="text-lg px-8 py-6 hover:bg-primary/10"
              onClick={() => document.getElementById('how-it-works')?.scrollIntoView({ behavior: 'smooth' })}
            >
              See How It Works
            </Button>
          </div>
          <div className="flex flex-wrap items-center justify-center gap-6 text-sm text-foreground/90 font-medium">
            <span className="flex items-center gap-2">
              <CheckCircle2 className="h-5 w-5 text-success" />
              100% Free Forever
            </span>
            <span className="flex items-center gap-2">
              <CheckCircle2 className="h-5 w-5 text-success" />
              No Credit Card Required
            </span>
            <span className="flex items-center gap-2">
              <CheckCircle2 className="h-5 w-5 text-success" />
              2 Minute Setup
            </span>
          </div>
        </div>
      </section>

      {/* Social Proof */}
      <section className="bg-card/50 backdrop-blur-sm py-8 border-y border-border">
        <div className="max-w-6xl mx-auto px-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 text-center">
            <div>
              <div className="text-4xl font-bold text-primary mb-2">1,200+</div>
              <div className="text-muted-foreground">Scholarships Tracked</div>
            </div>
            <div>
              <div className="text-4xl font-bold text-success mb-2">$45M+</div>
              <div className="text-muted-foreground">in Opportunities</div>
            </div>
            <div>
              <div className="text-4xl font-bold text-info mb-2">89%</div>
              <div className="text-muted-foreground">Success Rate</div>
            </div>
          </div>
        </div>
      </section>

      {/* How It Works */}
      <section id="how-it-works" className="py-24 px-4">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-4xl md:text-5xl font-bold text-center mb-4">
            Three Steps to Scholarship Success
          </h2>
          <p className="text-xl text-muted-foreground text-center mb-16">
            We make finding and winning scholarships effortless
          </p>
          
          <div className="grid md:grid-cols-3 gap-8">
            <Card className="p-8 hover:shadow-elegant transition-smooth hover:-translate-y-1">
              <div className="h-12 w-12 rounded-xl bg-primary/10 flex items-center justify-center mb-6">
                <User className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-2xl font-semibold mb-4">Tell Us About You</h3>
              <p className="text-muted-foreground leading-relaxed">
                Quick 2-minute profile. We ask about your background, academics, and interests.
              </p>
            </Card>

            <Card className="p-8 hover:shadow-elegant transition-smooth hover:-translate-y-1">
              <div className="h-12 w-12 rounded-xl bg-primary/10 flex items-center justify-center mb-6">
                <Sparkles className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-2xl font-semibold mb-4">Get Matched Instantly</h3>
              <p className="text-muted-foreground leading-relaxed">
                Our AI finds scholarships you qualify for, ranked by your probability of winning.
              </p>
            </Card>

            <Card className="p-8 hover:shadow-elegant transition-smooth hover:-translate-y-1">
              <div className="h-12 w-12 rounded-xl bg-primary/10 flex items-center justify-center mb-6">
                <Trophy className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-2xl font-semibold mb-4">Apply & Win</h3>
              <p className="text-muted-foreground leading-relaxed">
                Track deadlines, get AI essay help, manage applications. We guide you all the way.
              </p>
            </Card>
          </div>
        </div>
      </section>

      {/* Problem/Solution */}
      <section className="py-24 px-4 bg-background">
        <div className="max-w-6xl mx-auto">
          <div className="grid md:grid-cols-2 gap-8">
            <Card className="p-8 border-destructive bg-destructive/5">
              <h3 className="text-2xl font-bold mb-4 text-destructive">The Problem</h3>
              <p className="text-lg mb-6 text-foreground font-semibold">Students miss out on $2.9B yearly</p>
              <ul className="space-y-3">
                <li className="flex items-start gap-3">
                  <span className="text-destructive mt-1 text-xl">•</span>
                  <span className="text-foreground">Deadlines scattered across 10,000+ websites</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-destructive mt-1 text-xl">•</span>
                  <span className="text-foreground">No idea which scholarships you qualify for</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-destructive mt-1 text-xl">•</span>
                  <span className="text-foreground">Application fatigue from irrelevant opportunities</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-destructive mt-1 text-xl">•</span>
                  <span className="text-foreground">No reminders until it's too late</span>
                </li>
              </ul>
            </Card>

            <Card className="p-8 border-success bg-success/5">
              <h3 className="text-2xl font-bold mb-4 text-success">Our Solution</h3>
              <p className="text-lg mb-6 text-foreground font-semibold">ScholarStream solves all of this</p>
              <ul className="space-y-3">
                <li className="flex items-start gap-3">
                  <CheckCircle2 className="h-5 w-5 text-success mt-1 flex-shrink-0" />
                  <span className="text-foreground">AI matches you with perfect-fit scholarships</span>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle2 className="h-5 w-5 text-success mt-1 flex-shrink-0" />
                  <span className="text-foreground">Never miss a deadline with smart reminders</span>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle2 className="h-5 w-5 text-success mt-1 flex-shrink-0" />
                  <span className="text-foreground">Application co-pilot with essay assistance</span>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle2 className="h-5 w-5 text-success mt-1 flex-shrink-0" />
                  <span className="text-foreground">Track everything in one beautiful dashboard</span>
                </li>
              </ul>
            </Card>
          </div>
        </div>
      </section>

      {/* Final CTA */}
      <section className="gradient-primary py-24 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-4xl md:text-5xl font-bold mb-6 text-white">
            Ready to Find Your Scholarships?
          </h2>
          <p className="text-xl text-white/90 mb-8">
            Join 2,000+ students already using ScholarStream
          </p>
          <Button 
            size="lg" 
            className="bg-white text-primary hover:bg-white/90 text-lg px-8 py-6 shadow-lg animate-pulse-glow"
            onClick={() => navigate('/signup')}
          >
            Get Started Free
            <ArrowRight className="ml-2 h-5 w-5" />
          </Button>
          <p className="text-white/80 mt-6 text-sm">
            Takes 2 minutes • No credit card • Start applying today
          </p>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-card border-t border-border py-12 px-4">
        <div className="max-w-6xl mx-auto">
          <div className="flex flex-col md:flex-row justify-between items-center gap-6">
            <div>
              <h3 className="text-xl font-bold gradient-primary bg-clip-text text-transparent">
                ScholarStream
              </h3>
              <p className="text-sm text-muted-foreground">Your Personal Financial Aid Intelligence Engine</p>
            </div>
            <div className="flex gap-6 text-sm text-muted-foreground">
              <a href="#" className="hover:text-primary transition-colors">About</a>
              <a href="#" className="hover:text-primary transition-colors">Contact</a>
              <a href="#" className="hover:text-primary transition-colors">Privacy</a>
              <a href="#" className="hover:text-primary transition-colors">Terms</a>
            </div>
            <p className="text-sm text-muted-foreground">
              Made for Student Hackpad 2025 🎓
            </p>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Landing;