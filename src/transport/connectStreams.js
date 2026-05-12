// 把远端 socket 的下行流（含 BYOB 大缓冲分支）回灌到 WS / xhttpBridge / grpcBridge。
import { 数据转Uint8Array, 拼接字节数据 } from '../utils/bytes.js';
import { WebSocket发送并等待, closeSocketQuietly } from './socketUtils.js';
import { log } from '../runtime/log.js';

export async function connectStreams(remoteSocket, webSocket, headerData, retryFunc) {
	let header = headerData, hasData = false, reader, useBYOB = false;
	const BYOB缓冲区大小 = 512 * 1024, BYOB单次读取上限 = 64 * 1024, BYOB高吞吐阈值 = 50 * 1024 * 1024;
	const 普通流聚合阈值 = 128 * 1024, 普通流刷新间隔 = 2;
	const BYOB慢速刷新间隔 = 20, BYOB快速刷新间隔 = 2, BYOB安全阈值 = BYOB缓冲区大小 - BYOB单次读取上限;

	const 发送块 = async (chunk) => {
		if (webSocket.readyState !== WebSocket.OPEN) throw new Error('ws.readyState is not open');
		if (header) {
			const merged = new Uint8Array(header.length + chunk.byteLength);
			merged.set(header, 0); merged.set(chunk, header.length);
			await WebSocket发送并等待(webSocket, merged.buffer);
			header = null;
		} else await WebSocket发送并等待(webSocket, chunk);
	};

	try { reader = remoteSocket.readable.getReader({ mode: 'byob' }); useBYOB = true }
	catch (e) { reader = remoteSocket.readable.getReader() }

	try {
		if (!useBYOB) {
			let pendingChunks = [], pendingBytes = 0, flush定时器 = null, flush任务 = null;
			const flush = async () => {
				if (flush任务) return flush任务;
				flush任务 = (async () => {
					if (flush定时器) { clearTimeout(flush定时器); flush定时器 = null }
					if (pendingBytes <= 0) return;
					const chunks = pendingChunks, bytes = pendingBytes;
					pendingChunks = []; pendingBytes = 0;
					const payload = chunks.length === 1 ? chunks[0] : 拼接字节数据(...chunks);
					if (payload.byteLength || bytes > 0) await 发送块(payload);
				})().finally(() => { flush任务 = null });
				return flush任务;
			};
			const 推送普通流块 = async (chunk) => {
				const bytes = 数据转Uint8Array(chunk);
				if (!bytes.byteLength) return;
				pendingChunks.push(bytes);
				pendingBytes += bytes.byteLength;
				if (pendingBytes >= 普通流聚合阈值) {
					await flush();
					if (pendingBytes >= 普通流聚合阈值) await flush();
				} else if (!flush定时器) {
					flush定时器 = setTimeout(() => { flush().catch(() => closeSocketQuietly(webSocket)) }, 普通流刷新间隔);
				}
			};
			while (true) {
				const { done, value } = await reader.read();
				if (done) break;
				if (!value || value.byteLength === 0) continue;
				hasData = true;
				await 推送普通流块(value);
			}
			await flush();
		} else {
			let mainBuf = new ArrayBuffer(BYOB缓冲区大小), offset = 0, totalBytes = 0;
			let flush间隔毫秒 = BYOB快速刷新间隔, flush定时器 = null, 等待刷新恢复 = null;
			let 正在读取 = false, 读取中待刷新 = false;

			const flush = async () => {
				if (正在读取) { 读取中待刷新 = true; return }
				try {
					if (offset > 0) { const p = new Uint8Array(mainBuf.slice(0, offset)); offset = 0; await 发送块(p) }
				} finally {
					读取中待刷新 = false;
					if (flush定时器) { clearTimeout(flush定时器); flush定时器 = null }
					if (等待刷新恢复) { const r = 等待刷新恢复; 等待刷新恢复 = null; r() }
				}
			};

			while (true) {
				正在读取 = true;
				const { done, value } = await reader.read(new Uint8Array(mainBuf, offset, BYOB单次读取上限));
				正在读取 = false;
				if (done) break;
				if (!value || value.byteLength === 0) { if (读取中待刷新) await flush(); continue }
				hasData = true;
				mainBuf = value.buffer;
				const len = value.byteLength;

				if (value.byteOffset !== offset) {
					log(`[BYOB] 偏移异常: 预期=${offset}, 实际=${value.byteOffset}`);
					await 发送块(new Uint8Array(value.buffer, value.byteOffset, len).slice());
					mainBuf = new ArrayBuffer(BYOB缓冲区大小); offset = 0; totalBytes = 0;
					continue;
				}

				if (len < BYOB单次读取上限) {
					flush间隔毫秒 = BYOB快速刷新间隔;
					if (len < 4096) totalBytes = 0;
					if (offset > 0) { offset += len; await flush() }
					else await 发送块(value.slice());
				} else {
					totalBytes += len; offset += len;
					if (!flush定时器) flush定时器 = setTimeout(() => { flush().catch(() => closeSocketQuietly(webSocket)) }, flush间隔毫秒);
					if (读取中待刷新) await flush();
					if (offset > BYOB安全阈值) {
						if (totalBytes > BYOB高吞吐阈值) flush间隔毫秒 = BYOB慢速刷新间隔;
						await new Promise(r => { 等待刷新恢复 = r });
					}
				}
			}
			正在读取 = false;
			await flush();
			if (flush定时器) { clearTimeout(flush定时器); flush定时器 = null }
		}
	} catch (err) { closeSocketQuietly(webSocket) }
	finally { try { reader.cancel() } catch (e) { } try { reader.releaseLock() } catch (e) { } }
	if (!hasData && retryFunc) await retryFunc();
}
