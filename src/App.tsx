import React, { useState, useRef, useEffect } from 'react';
import { Terminal as TerminalIcon, Settings, Info, Server, Play, Trash2, CheckCircle2, XCircle } from 'lucide-react';
import { CommandEntry } from './types';

export default function App() {
  const [endpoint, setEndpoint] = useState('http://localhost:8080/api/command');
  const [command, setCommand] = useState('');
  const [history, setHistory] = useState<CommandEntry[]>([
    {
      id: '1',
      type: 'system',
      content: 'Welcome to the Redis Web CLI.\nType "help" for a list of local commands or enter a Redis command to send to your Java backend.',
      timestamp: new Date()
    }
  ]);
  const [isConnecting, setIsConnecting] = useState(false);
  
  const endOfTerminalRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Auto-scroll to bottom of terminal when history changes
  useEffect(() => {
    endOfTerminalRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [history]);

  // Focus input on load
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const addHistory = (type: CommandEntry['type'], content: string) => {
    setHistory(prev => [...prev, {
      id: Math.random().toString(36).substring(2, 9),
      type,
      content,
      timestamp: new Date()
    }]);
  };

  const handleCommand = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!command.trim()) return;

    const cmd = command.trim();
    setCommand('');
    addHistory('input', cmd);

    // Handle local CLI commands
    if (cmd.toLowerCase() === 'clear') {
      setHistory([]);
      return;
    }
    if (cmd.toLowerCase() === 'help') {
      addHistory('system', 'Local commands:\n  clear - Clear the terminal screen\n  help  - Show this message\n\nAll other commands will be forwarded to your Java REST backend via POST.');
      return;
    }

    // Forward command to Java REST backend
    setIsConnecting(true);
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ command: cmd }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      
      // Assume the backend returns a JSON object like { "result": "OK" } or { "result": "..." }
      // Adjust this based on how you actually build your Java backend
      const resultText = data.result !== undefined ? String(data.result) : JSON.stringify(data);
      addHistory('output', resultText);
      
    } catch (err: any) {
      addHistory('error', `Failed to connect to backend: ${err.message}\nMake sure your Java server is running and CORS is enabled.`);
    } finally {
      setIsConnecting(false);
    }
  };

  return (
    <div className="flex h-screen w-full bg-zinc-950 text-zinc-300 font-sans overflow-hidden">
      
      {/* Sidebar - Settings & Info */}
      <aside className="w-80 bg-zinc-900 border-r border-zinc-800 flex flex-col z-10 shrink-0">
        <div className="p-6 border-b border-zinc-800 flex items-center gap-3">
          <div className="bg-emerald-500/10 text-emerald-400 p-2 rounded-lg">
            <DatabaseIcon className="w-5 h-5" />
          </div>
          <div>
            <h1 className="text-zinc-100 font-semibold tracking-tight">Redis Web CLI</h1>
            <p className="text-xs text-zinc-500">Custom Java Backend UI</p>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-6 space-y-8">
          
          {/* Connection Settings */}
          <section className="space-y-3">
            <div className="flex items-center gap-2 text-zinc-100 font-medium">
              <Settings className="w-4 h-4 text-zinc-400" />
              <h2>Connection Settings</h2>
            </div>
            <div className="space-y-1.5">
              <label className="text-xs text-zinc-500 font-medium ml-1">REST API Endpoint</label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <Server className="w-4 h-4 text-zinc-500" />
                </div>
                <input
                  type="text"
                  value={endpoint}
                  onChange={(e) => setEndpoint(e.target.value)}
                  className="w-full bg-zinc-950 border border-zinc-800 rounded-lg py-2 pl-9 pr-3 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/50 focus:border-emerald-500 transition-all text-zinc-300"
                  placeholder="http://localhost:8080/api/command"
                />
              </div>
            </div>
          </section>

          {/* Java Backend Contract */}
          <section className="space-y-3">
            <div className="flex items-center gap-2 text-zinc-100 font-medium">
              <Info className="w-4 h-4 text-zinc-400" />
              <h2>Java Backend Contract</h2>
            </div>
            
            <div className="bg-zinc-950 rounded-lg border border-zinc-800 p-4 text-sm space-y-4">
              <p className="text-zinc-400 text-xs leading-relaxed">
                Build your Java REST controller to accept these incoming requests:
              </p>
              
              <div>
                <div className="text-xs font-semibold text-zinc-500 mb-1">REQUEST (POST)</div>
                <pre className="text-[11px] bg-zinc-900 p-2 rounded text-zinc-300 font-mono overflow-x-auto">
{`{
  "command": "SET mykey Hello"
}`}
                </pre>
              </div>

              <div>
                <div className="text-xs font-semibold text-zinc-500 mb-1">EXPECTED RESPONSE</div>
                <pre className="text-[11px] bg-zinc-900 p-2 rounded text-emerald-400 font-mono overflow-x-auto">
{`{
  "result": "OK"
}`}
                </pre>
              </div>

              <div className="mt-4 pt-4 border-t border-zinc-800">
                <div className="flex items-start gap-2 text-amber-500/80">
                  <Info className="w-4 h-4 mt-0.5 shrink-0" />
                  <p className="text-xs">
                    Remember to configure <strong>CORS</strong> in your Java backend to allow requests from this frontend domain.
                  </p>
                </div>
              </div>
            </div>
          </section>
        </div>
      </aside>

      {/* Main Terminal Area */}
      <main className="flex-1 flex flex-col bg-zinc-950 relative" onClick={() => inputRef.current?.focus()}>
        {/* Terminal Header */}
        <header className="h-14 border-b border-zinc-800 flex items-center px-6 justify-between shrink-0 bg-zinc-950/80 backdrop-blur-sm z-10 sticky top-0">
          <div className="flex items-center gap-2 text-sm text-zinc-400">
            <TerminalIcon className="w-4 h-4" />
            <span className="font-mono">redis-cli &mdash; {endpoint}</span>
          </div>
          
          <div className="flex items-center gap-4">
            <button 
              onClick={() => setHistory([])}
              className="text-xs flex items-center gap-1.5 text-zinc-500 hover:text-zinc-300 transition-colors"
              title="Clear Terminal"
            >
              <Trash2 className="w-3.5 h-3.5" />
              Clear
            </button>
            <div className="h-3 w-px bg-zinc-800"></div>
            <div className="flex items-center gap-1.5 text-xs font-medium">
              {isConnecting ? (
                <span className="flex items-center gap-1.5 text-amber-400">
                  <div className="w-2 h-2 rounded-full border border-amber-400 border-t-transparent animate-spin"></div>
                  Sending...
                </span>
              ) : (
                <span className="flex items-center gap-1.5 text-emerald-400">
                  <CheckCircle2 className="w-3.5 h-3.5" />
                  Ready
                </span>
              )}
            </div>
          </div>
        </header>

        {/* Terminal Output */}
        <div className="flex-1 overflow-y-auto p-6 font-mono text-sm">
          <div className="space-y-3 pb-4 max-w-4xl">
            {history.map((entry) => (
              <div key={entry.id} className="flex gap-4 group">
                
                {/* Timestamp */}
                <div className="w-20 text-right text-zinc-600 text-xs mt-0.5 shrink-0 select-none">
                  {entry.timestamp.toLocaleTimeString([], { hour12: false })}
                </div>

                {/* Content */}
                <div className="flex-1 whitespace-pre-wrap break-words">
                  {entry.type === 'input' && (
                    <div className="flex gap-2 text-zinc-100">
                      <span className="text-zinc-500 select-none">&gt;</span>
                      <span className="font-semibold text-emerald-300">{entry.content}</span>
                    </div>
                  )}
                  
                  {entry.type === 'output' && (
                    <div className="text-zinc-300 pl-4 py-1">
                      {entry.content}
                    </div>
                  )}
                  
                  {entry.type === 'error' && (
                    <div className="text-red-400 pl-4 py-1 flex items-start gap-2">
                      <XCircle className="w-4 h-4 mt-0.5 shrink-0 opacity-80" />
                      <span>{entry.content}</span>
                    </div>
                  )}
                  
                  {entry.type === 'system' && (
                    <div className="text-zinc-500 italic pl-4 py-1">
                      {entry.content}
                    </div>
                  )}
                </div>
              </div>
            ))}
            <div ref={endOfTerminalRef} />
          </div>
        </div>

        {/* Terminal Input */}
        <div className="p-4 bg-zinc-950 border-t border-zinc-900 shrink-0">
          <form onSubmit={handleCommand} className="relative max-w-4xl">
            <div className="absolute inset-y-0 left-0 pl-4 flex items-center pointer-events-none">
              <span className="text-emerald-500 font-mono font-bold text-lg">&gt;</span>
            </div>
            <input
              ref={inputRef}
              type="text"
              value={command}
              onChange={(e) => setCommand(e.target.value)}
              disabled={isConnecting}
              className="w-full bg-zinc-900/50 border border-zinc-800 rounded-lg py-3 pl-10 pr-12 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500/50 focus:border-emerald-500 transition-all text-zinc-100 disabled:opacity-50"
              placeholder="Enter Redis command (e.g. SET mykey 'hello')"
              autoComplete="off"
              spellCheck="false"
            />
            <button
              type="submit"
              disabled={!command.trim() || isConnecting}
              className="absolute inset-y-1.5 right-1.5 px-3 bg-emerald-500/10 text-emerald-400 rounded hover:bg-emerald-500/20 disabled:opacity-30 disabled:hover:bg-emerald-500/10 transition-colors flex items-center justify-center"
            >
              <Play className="w-4 h-4 fill-current" />
            </button>
          </form>
        </div>
      </main>
    </div>
  );
}

// Simple DB Icon component since lucide Database isn't explicitly imported
function DatabaseIcon(props: any) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M3 5V19A9 3 0 0 0 21 19V5" />
      <path d="M3 12A9 3 0 0 0 21 12" />
    </svg>
  );
}

