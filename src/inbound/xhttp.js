// XHTTP 入站（双向流，请求体 + 响应体都是 ReadableStream）。
import { forwardataTCP } from '../transport/forwardTcp.js';
import { forwardataudp } from '../transport/forwardUdp.js';
import { 转发木马UDP数据 } from '../transport/forwardTrojanUdp.js';
import { closeSocketQuietly } from '../transport/socketUtils.js';
import { isSpeedTestSite } from '../transport/speedtestBlock.js';
import { log } from '../runtime/log.js';
import { 读取XHTTP首包 } from './xhttpHeader.js';

export async function 处理XHTTP请求(ctx, request, yourUUID) {
	if (!request.body) return new Response('Bad Request', { status: 400 });
	const reader = request.body.getReader();
	const 首包 = await 读取XHTTP首包(reader, yourUUID);
	if (!首包) {
		try { reader.releaseLock() } catch (e) { }
		return new Response('Invalid request', { status: 400 });
	}
	if (isSpeedTestSite(首包.hostname)) {
		try { reader.releaseLock() } catch (e) { }
		return new Response('Forbidden', { status: 403 });
	}
	if (首包.isUDP && 首包.协议 !== 'trojan' && 首包.port !== 53) {
		try { reader.releaseLock() } catch (e) { }
		return new Response('UDP is not supported', { status: 400 });
	}

	const remoteConnWrapper = { socket: null, connectingPromise: null, retryConnect: null };
	let 当前写入Socket = null;
	let 远端写入器 = null;
	const responseHeaders = new Headers({
		'Content-Type': 'application/octet-stream',
		'X-Accel-Buffering': 'no',
		'Cache-Control': 'no-store'
	});

	const 释放远端写入器 = () => {
		if (远端写入器) {
			try { 远端写入器.releaseLock() } catch (e) { }
			远端写入器 = null;
		}
		当前写入Socket = null;
	};

	const 获取远端写入器 = () => {
		const socket = remoteConnWrapper.socket;
		if (!socket) return null;
		if (socket !== 当前写入Socket) {
			释放远端写入器();
			当前写入Socket = socket;
			远端写入器 = socket.writable.getWriter();
		}
		return 远端写入器;
	};

	return new Response(new ReadableStream({
		async start(controller) {
			let 已关闭 = false;
			let udpRespHeader = 首包.respHeader;
			const 木马UDP上下文 = { 缓存: new Uint8Array(0) };
			const xhttpBridge = {
				readyState: WebSocket.OPEN,
				send(data) {
					if (已关闭) return;
					try {
						const chunk = data instanceof Uint8Array
							? data
							: data instanceof ArrayBuffer
								? new Uint8Array(data)
								: ArrayBuffer.isView(data)
									? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
									: new Uint8Array(data);
						controller.enqueue(chunk);
					} catch (e) {
						已关闭 = true;
						this.readyState = WebSocket.CLOSED;
					}
				},
				close() {
					if (已关闭) return;
					已关闭 = true;
					this.readyState = WebSocket.CLOSED;
					try { controller.close() } catch (e) { }
				}
			};

			const 写入远端 = async (payload, allowRetry = true) => {
				const writer = 获取远端写入器();
				if (!writer) return false;
				try {
					await writer.write(payload);
					return true;
				} catch (err) {
					释放远端写入器();
					if (allowRetry && typeof remoteConnWrapper.retryConnect === 'function') {
						await remoteConnWrapper.retryConnect();
						return await 写入远端(payload, false);
					}
					throw err;
				}
			};

			try {
				if (首包.isUDP) {
					if (首包.rawData?.byteLength) {
						if (首包.协议 === 'trojan') await 转发木马UDP数据(首包.rawData, xhttpBridge, 木马UDP上下文);
						else await forwardataudp(首包.rawData, xhttpBridge, udpRespHeader);
						udpRespHeader = null;
					}
				} else {
					await forwardataTCP(首包.hostname, 首包.port, 首包.rawData, xhttpBridge, 首包.respHeader, remoteConnWrapper, yourUUID);
				}

				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					if (!value || value.byteLength === 0) continue;
					if (首包.isUDP) {
						if (首包.协议 === 'trojan') await 转发木马UDP数据(value, xhttpBridge, 木马UDP上下文);
						else await forwardataudp(value, xhttpBridge, udpRespHeader);
						udpRespHeader = null;
					} else {
						if (!(await 写入远端(value))) throw new Error('Remote socket is not ready');
					}
				}

				if (!首包.isUDP) {
					const writer = 获取远端写入器();
					if (writer) {
						try { await writer.close() } catch (e) { }
					}
				}
			} catch (err) {
				log(`[XHTTP转发] 处理失败: ${err?.message || err}`);
				closeSocketQuietly(xhttpBridge);
			} finally {
				释放远端写入器();
				try { reader.releaseLock() } catch (e) { }
			}
		},
		cancel() {
			释放远端写入器();
			try { remoteConnWrapper.socket?.close() } catch (e) { }
			try { reader.releaseLock() } catch (e) { }
		}
	}), { status: 200, headers: responseHeaders });
}

