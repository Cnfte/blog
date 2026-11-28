/* audio-accel-sw.js
 * ────────────────────────────────────────────────────────────────
 * 音频分片并行加速代理（Service Worker）。放在 themes/default/ 下，构建时和
 * index.html / post.html 一样被复制到 public/ 根目录；本地面板预览由 main.py
 * 另开一个路由直接读这份源文件返回。
 *
 * 解决的问题：国内访问 Cloudflare / GitHub Pages 等海外托管资源时，单条 HTTP
 * 连接经常被限速到只有几 KB/s，几 MB 的歌曲文件单流下载会很慢。但同源开几条
 * 并行 Range 请求分段拉取，聚合吞吐明显更好（原理跟下载器的多线程分段下载
 * 一样）。
 *
 * 关键点：这不是"全部下完再播"——那样等待时间比浏览器原生流式加载更差，只是
 * 换了个"进度条"而已，不是真正的加速。这里做的是：<audio> 元素发起的每一次
 * 请求（不管带不带 Range，包括拖进度条产生的新请求）都被这个 SW 拦截；我们把
 * 这次请求要的字节范围拆成 N 份，同时并行向真实源站发起子请求，但严格按顺序
 * 把各段数据依次写回给 <audio>——下载在后台并行进行，播放端看到的始终是一条
 * 完整有序的字节流，下载到哪个字节就能播到哪个字节，不需要等整个文件下完。
 *
 * 首次点击到出声的延迟优化：早先版本在真正拉数据之前，会先单独发一个
 * "Range: bytes=0-1" 的探测请求去问文件大小 / 是否支持 Range，等它完整返回
 * 之后才开始发真正喂给 <audio> 的数据请求——等于每首歌第一次播放都要白白
 * 多等一个网络往返（在高延迟线路上这一步可能就是几百毫秒）。现在把探测和
 * "拿第一块真实数据"合并成同一个请求：直接按 Range 去要第一小块数据，
 * 大小/是否支持 Range 这些信息从这次请求的响应头（Content-Range /
 * Content-Length / status）里顺带拿到，省掉那次专门的探测往返。第一块数据
 * 一到就能立刻喂给 <audio> 开始播放，剩余部分再并行加速下载、按顺序接上。
 *
 * 任何一步出问题（源站不支持 Range、子请求失败、探测失败等）都直接透传原始
 * 请求，退回浏览器原生单流加载——加速逻辑本身的问题绝不能导致播放失败。
 */
'use strict';

const CHUNK_COUNT = 4;              // 已知文件大小后，剩余部分的并行子请求数（不宜太多，避免被源站/CDN 判定异常连接数）
const MIN_SPLIT_SIZE = 512 * 1024;  // 请求范围小于这个大小时不值得拆分并行
const FIRST_CHUNK_SIZE = 256 * 1024; // 首块大小：足够快到达（尽量缩短点击到出声的时间），
                                      // 又足够撑起 <audio> 先播起来、不至于立刻断流等下一块
const PARALLEL_START_DELAY_MS = 400; // "剩余部分"的并行请求延迟这么久再发。
                                      // 之前是首块请求一发出去，剩余部分的并行请求就立刻跟着一起发——
                                      // 在真正的带宽瓶颈场景（比如跨境线路整体被限速到几 KB/s，
                                      // 不是单连接限速而是这条线路总吞吐就这么多）下，这些并行连接
                                      // 会跟首块抢同一根本就不宽裕的管道，首块反而更慢到，"点击到
                                      // 出声"的等待时间比不做加速还长。留这么点延迟，让首块独享带宽
                                      // 尽快落地、<audio> 尽快开始播放，剩余部分再并行补上，两边不打架。
const STAGGER_STEP_MS = 350;         // 关键修复：上面这条"别跟首块抢带宽"的道理，同样适用于
                                      // "剩余部分"内部——原来剩余的几个分片是同一时刻一起发出去的，
                                      // 但其中只有紧接着首块之后的那一片是播放马上要用到的，后面几片
                                      // 还早。在总吞吐真的被卡死（而不是单连接限速）的跨境弱网下，
                                      // 这几片会平分本就不多的带宽，导致"马上要用"的那片反而只分到
                                      // 1/3 左右的速度，实际观感就是明明开了好几条并行连接，却要等
                                      // 下载完 1MB 多、老半天才能听到声音——比不加速还慢。这里让"剩余
                                      // 部分"里的每一片再依次错开一段时间才发出（越晚需要的片延迟越
                                      // 久），把带宽优先让给真正快要播到的那一片，后面的片再逐步跟上，
                                      // 而不是一次性挤爆管道。
const MAX_CONCURRENT_FETCHES = 4;    // 关键修复二：SW 自己开的并行分片请求，不能只在"单首歌内部"
                                      // 控制数量——当前播放的歌最多同时开 CHUNK_COUNT 条，如果这时候
                                      // prefetchAdjacent() 又在后台预取下一首（同样会被这个 SW 拦截、
                                      // 同样走并行分片），两边一叠加很容易超过浏览器对同一个域名的
                                      // 并发连接数上限（HTTP/1.1 常见 6 条/域名，多层 CDN 转发时这个
                                      // 上限可能更容易被触发）。超限之后，浏览器会把多出来的请求晾在
                                      // 内部队列里，表现为某个分片请求"排队"十几秒都发不出去——跟没加速
                                      // 甚至更差。这里用一个全局并发闸门，把本 SW 发起的所有分片请求
                                      // （不管来自哪首歌、当前播放的还是预取的）总数卡住，宁可局部串行
                                      // 一点，也不去挤爆连接数上限。
let _activeFetches = 0;
const _fetchQueue = [];

// 所有 SW 自己发起的分片子请求都要走这里，而不是直接调 fetch()——用一个简单的
// 令牌队列把全局并发数控制在 MAX_CONCURRENT_FETCHES 以内，排队等到有空位再真正
// 发出网络请求；跟直接用 fetch() 相比只是"何时真正发出"不同，用法和返回值一样。
function gatedFetch(url, opts) {
    return new Promise((resolve, reject) => {
        const run = () => {
            _activeFetches++;
            fetch(url, opts).then(
                res => { _activeFetches--; _drainFetchQueue(); resolve(res); },
                err => { _activeFetches--; _drainFetchQueue(); reject(err); }
            );
        };
        if (_activeFetches < MAX_CONCURRENT_FETCHES) run();
        else _fetchQueue.push(run);
    });
}

function _drainFetchQueue() {
    if (_fetchQueue.length && _activeFetches < MAX_CONCURRENT_FETCHES) {
        _fetchQueue.shift()();
    }
}
const sizeCache = new Map();        // url -> { acceptRanges, size }（Service Worker 存活期间有效，重启后重新探测即可）

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));

function isAudioRequest(url) {
    return /\.(mp3|flac)(\?|$)/i.test(url) &&
        (url.indexOf('/audio/files/') !== -1 || url.indexOf('/api/audio/stream/') !== -1);
}

function splitRange(start, end, n) {
    const size = end - start + 1;
    const chunk = Math.ceil(size / n);
    const parts = [];
    for (let i = 0; i < n; i++) {
        const s = start + i * chunk;
        if (s > end) break;
        parts.push([s, Math.min(s + chunk - 1, end)]);
    }
    return parts.length ? parts : [[start, end]];
}

// 跟 fetch() 用法一样，只是延迟 delayMs 之后才真正发出请求——用来给首块请求
// 留一个独享带宽的窗口，见 PARALLEL_START_DELAY_MS 的注释。延迟结束后走
// gatedFetch()，同样受全局并发闸门控制。
function delayedFetch(url, opts, delayMs) {
    if (!delayMs) return gatedFetch(url, opts);
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            gatedFetch(url, opts).then(resolve, reject);
        }, delayMs);
    });
}

// 把一组"响应 Promise"（可以是已经在飞行中的、也可以是尚未发出的 fetch）严格
// 按数组顺序拼接成一条有序字节流。谁先到达不重要（背后可能是好几条并行连接），
// 输出永远按 fetchPromises 的顺序，<audio> 感知不到背后其实是分片拼出来的。
function streamParts(fetchPromises) {
    let idx = 0;
    let reader = null;
    return new ReadableStream({
        async pull(controller) {
            for (;;) {
                if (!reader) {
                    if (idx >= fetchPromises.length) { controller.close(); return; }
                    let res;
                    try {
                        res = await fetchPromises[idx];
                    } catch (e) {
                        controller.error(e);
                        return;
                    }
                    if (res.status !== 206 && res.status !== 200) {
                        controller.error(new Error('分片请求失败: ' + res.status));
                        return;
                    }
                    reader = res.body.getReader();
                }
                const { done, value } = await reader.read();
                if (done) { reader = null; idx++; continue; }
                controller.enqueue(value);
                return; // 每次只吐一小块给底层，交回控制权，让背压机制正常工作
            }
        }
    });
}

// 已经知道文件大小/支持 Range（sizeCache 命中）时使用：请求的范围直接拆成
// CHUNK_COUNT 份，第一份立即发出（谁在等这段数据谁优先），其余几份依次错开
// （第 idx 份延迟 PARALLEL_START_DELAY_MS + (idx-1)*STAGGER_STEP_MS 再发，
// 而不是全部延迟同一个时间点一起发）——原因见 STAGGER_STEP_MS 的注释：
// 严格按顺序消费的分片里，只有紧跟在当前分片后面那个是马上要用的，其余的
// 还早，不该跟它抢带宽，让它们分批依次加入而不是同时挤爆管道。
function parallelStream(url, start, end, signal) {
    const subs = splitRange(start, end, CHUNK_COUNT);
    const fetches = subs.map(([s, e], idx) =>
        idx === 0
            ? gatedFetch(url, { headers: { Range: `bytes=${s}-${e}` }, signal })
            : delayedFetch(url, { headers: { Range: `bytes=${s}-${e}` }, signal },
                PARALLEL_START_DELAY_MS + (idx - 1) * STAGGER_STEP_MS));
    return streamParts(fetches);
}

// 计算本次请求实际需要覆盖到的结束字节：有 Range 头就按 Range 头（开放结尾则到
// 文件末尾），没有 Range 头（少数老式场景）就是整个文件。
function resolveNeededEnd(rangeHeader, totalSize) {
    if (!rangeHeader) return totalSize - 1;
    const m = /bytes=(\d*)-(\d*)/.exec(rangeHeader);
    if (m && m[2] !== '') return Math.min(parseInt(m[2], 10), totalSize - 1);
    return totalSize - 1;
}

async function handleAudioRequest(request) {
    const url = request.url;
    const rangeHeader = request.headers.get('Range');
    const contentType = /\.flac(\?|$)/i.test(url) ? 'audio/flac' : 'audio/mpeg';

    const cached = sizeCache.get(url);

    if (cached) {
        if (!cached.acceptRanges || !cached.size) return fetch(request); // 已知源站不支持 Range，直接透传
        return serveWithKnownSize(request, url, rangeHeader, cached.size, contentType);
    }

    // 这个 URL 是第一次被请求（比如这首歌第一次点开）：不再单独发探测包，
    // 直接把"第一小块真实数据"的请求发出去，顺便从它的响应头里拿到探测信息。
    return serveFirstRequest(request, url, rangeHeader, contentType);
}

async function serveWithKnownSize(request, url, rangeHeader, totalSize, contentType) {
    let start = 0, end = totalSize - 1;
    if (rangeHeader) {
        const m = /bytes=(\d*)-(\d*)/.exec(rangeHeader);
        if (m) {
            if (m[1] === '' && m[2] !== '') {
                // "bytes=-500" 这种"最后 N 字节"写法，<audio> 基本用不到，按需支持一下
                start = Math.max(0, totalSize - parseInt(m[2], 10));
                end = totalSize - 1;
            } else {
                if (m[1] !== '') start = parseInt(m[1], 10);
                if (m[2] !== '') end = parseInt(m[2], 10);
            }
        }
    }
    if (start >= totalSize) {
        return new Response(null, { status: 416, headers: { 'Content-Range': `bytes */${totalSize}` } });
    }
    end = Math.min(end, totalSize - 1);

    const total = end - start + 1;
    if (total < MIN_SPLIT_SIZE) {
        // 这次请求的范围本身很小（比如临近结尾的最后一小段），拆分反而增加开销，
        // 直接单条透传，仍然是一次正常的原生 Range 请求
        return fetch(request);
    }

    const stream = parallelStream(url, start, end, request.signal);
    const headers = new Headers({
        'Content-Range': `bytes ${start}-${end}/${totalSize}`,
        'Content-Length': String(total),
        'Accept-Ranges': 'bytes',
        'Content-Type': contentType,
    });
    return new Response(stream, { status: 206, headers });
}

async function serveFirstRequest(request, url, rangeHeader, contentType) {
    // "bytes=-N"（最后 N 字节）这种写法几乎不会出现在一首歌的第一次请求里，
    // 为这种冷门 case 专门处理不划算，直接透传给浏览器原生处理更省心。
    if (rangeHeader && /bytes=-\d+/.test(rangeHeader)) {
        return fetch(request);
    }
    let start = 0;
    if (rangeHeader) {
        const m = /bytes=(\d+)-/.exec(rangeHeader);
        if (m) start = parseInt(m[1], 10);
    }
    const firstEnd = start + FIRST_CHUNK_SIZE - 1;

    let firstRes;
    try {
        firstRes = await fetch(url, { headers: { Range: `bytes=${start}-${firstEnd}` }, signal: request.signal });
    } catch (_) {
        return fetch(request); // 连第一块都请求失败，直接退回原生加载兜底
    }

    if (firstRes.status !== 206) {
        // 源站不支持 Range（忽略了 Range 头，直接把整份 200 吐回来了）：
        // 记下"不支持"，以后这个 URL 不用再走加速逻辑；这次响应本身已经是一次
        // 完整、可原生流式播放的响应，直接原样返回给 <audio>，不用再包一层。
        sizeCache.set(url, { acceptRanges: false, size: 0 });
        return firstRes;
    }

    const cr = firstRes.headers.get('Content-Range');
    let totalSize = 0;
    if (cr) {
        const m = /\/(\d+)\s*$/.exec(cr);
        if (m) totalSize = parseInt(m[1], 10);
    }
    if (!totalSize) {
        // 拿不到总大小就没法继续做并行分片，把已经在飞行中的这个首块响应
        // 原样透传出去，仍然能播，只是没有后续的并行加速
        sizeCache.set(url, { acceptRanges: false, size: 0 });
        return firstRes;
    }
    sizeCache.set(url, { acceptRanges: true, size: totalSize });

    const neededEnd = resolveNeededEnd(rangeHeader, totalSize);
    const actualFirstEnd = Math.min(firstEnd, neededEnd, totalSize - 1);

    // 首块请求已经在飞了（数据说不定已经开始到达），剩余部分（如果还有）现在才
    // 需要决定要不要拆成多条并行——首块负责"尽快出声"，剩余部分负责"整体提速"。
    //
    // 剩余部分内部同样要依次错开发出（第 idx 份延迟
    // PARALLEL_START_DELAY_MS + idx*STAGGER_STEP_MS），不能全部挤在同一时刻——
    // streamParts() 是严格按数组顺序把各片依次交给 <audio> 的，紧跟首块之后的
    // 那一片是马上要播到的，后面几片还早。如果像之前那样所有剩余分片同一时刻
    // 一起发出，在总吞吐被卡死的跨境弱网下它们会平分本就不宽裕的带宽，反而拖慢
    // 了"马上要播到的那一片"——表现出来就是明明在"加速"，实际上要下载 1MB 多
    // 才等到能播的数据，比不加速的原生单流还慢。错开发出能保证带宽优先让给
    // 真正快要播到的那一片，后面的片再逐步跟上补齐。
    const parts = [Promise.resolve(firstRes)];
    if (actualFirstEnd < neededEnd) {
        const remainStart = actualFirstEnd + 1;
        const remainSize = neededEnd - remainStart + 1;
        if (remainSize < MIN_SPLIT_SIZE) {
            parts.push(delayedFetch(url, { headers: { Range: `bytes=${remainStart}-${neededEnd}` }, signal: request.signal }, PARALLEL_START_DELAY_MS));
        } else {
            const subs = splitRange(remainStart, neededEnd, Math.max(1, CHUNK_COUNT - 1));
            subs.forEach(([s, e], idx) => {
                parts.push(delayedFetch(url, { headers: { Range: `bytes=${s}-${e}` }, signal: request.signal },
                    PARALLEL_START_DELAY_MS + idx * STAGGER_STEP_MS));
            });
        }
    }

    const stream = streamParts(parts);
    const headers = new Headers({
        'Content-Range': `bytes ${start}-${neededEnd}/${totalSize}`,
        'Content-Length': String(neededEnd - start + 1),
        'Accept-Ranges': 'bytes',
        'Content-Type': contentType,
    });
    return new Response(stream, { status: 206, headers });
}

self.addEventListener('fetch', event => {
    const req = event.request;
    if (req.method !== 'GET' || !isAudioRequest(req.url)) return; // 非音频请求完全不拦截，走浏览器默认行为

    // <link rel="preload" as="audio"> 预取下一首歌的请求（见 prefetchAdjacent()）
    // 会带上 Sec-Purpose/Purpose: prefetch 头。预取是"提前存进磁盘缓存"，不追求
    // 立刻出声，没必要跟当前正在播放的曲目抢并发连接名额——直接放行走浏览器
    // 原生单流加载即可，省下的连接数全部留给真正在播的那首歌。
    const purpose = req.headers.get('Sec-Purpose') || req.headers.get('Purpose') || '';
    if (/prefetch/i.test(purpose)) return;

    event.respondWith(handleAudioRequest(req).catch(() => fetch(req)));
});