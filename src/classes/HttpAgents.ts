import https from 'https';
import dns from 'dns';

interface DnsCacheEntry
{
    address: string;
    family: number;
    expiresAt: number;
}

/**
 * Process-level DNS result cache shared by all agents.
 * This app contacts only a small fixed set of hostnames, so no LRU eviction is needed.
 * Failed lookups are deliberately NOT cached so the next attempt retries the resolver immediately.
 */
const dnsCache = new Map<string, DnsCacheEntry>();
const DNS_TTL_MS = 30_000; // 30 seconds — short enough to pick up real DNS changes, long enough to absorb burst storms

/**
 * Drop-in replacement for dns.lookup that caches successful results for DNS_TTL_MS.
 * Intended as the `lookup` hook on https.Agent so every new TCP connection reuses
 * the cached address instead of hitting the OS resolver on each request.
 */
function cachedLookup(
    hostname: string,
    options: dns.LookupOptions,
    callback: (err: NodeJS.ErrnoException | null, address: string, family: number) => void
): void
{
    const cached = dnsCache.get(hostname);
    if(cached && Date.now() < cached.expiresAt)
    {
        callback(null, cached.address, cached.family);
        return;
    }

    // Force all:false so we always get the single-address overload of dns.lookup.
    // https.Agent never passes all:true but the union type requires we handle it.
    dns.lookup(hostname, { ...options, all: false } as dns.LookupOneOptions, (err, address, family) => {
        if(!err)
            dnsCache.set(hostname, { address, family, expiresAt: Date.now() + DNS_TTL_MS });
        callback(err, address, family);
    });
}

const sharedOptions = {
    keepAlive: true,
    maxFreeSockets: 4,
    timeout: 30_000,         // Destroy idle pooled sockets after 30 s of inactivity
    scheduling: 'lifo' as const, // Reuse the most-recently-active socket first to keep fewer sockets warm
    lookup: cachedLookup,
};

/**
 * Persistent HTTPS connection pool for Microsoft Graph API (graph.microsoft.com).
 * maxSockets matches Mailer's Semaphore(4) so there is never a queue inside the agent.
 */
export const graphAgent = new https.Agent({
    ...sharedOptions,
    maxSockets: 4,
} as unknown as https.AgentOptions);

/**
 * Persistent HTTPS connection pool for MSAL token acquisition (login.microsoftonline.com).
 * Token requests are serialised by a mutex so 2 sockets is generous.
 */
export const msalAgent = new https.Agent({
    ...sharedOptions,
    maxSockets: 2,
    maxFreeSockets: 2,
} as unknown as https.AgentOptions);
