// gRPC 入站（实际上是 HTTP/2 流式 + protobuf 长度前缀帧）。
import { 解析木马请求, 解析魏烈思请求 } from './headerParser.js';
import { forwardataTCP } from '../transport/forwardTcp.js';
import { forwardataudp } from '../transport/forwardUdp.js';
import { 转发木马UDP数据 } from '../transport/forwardTrojanUdp.js';
import { isSpeedTestSite } from '../transport/speedtestBlock.js';
import { 有效数据长度 } from '../utils/bytes.js';
import { log } from '../runtime/log.js';

export async function 处理gRPC请求(ctx, request, yourUUID) {
	if (!request.body) return new Response('Bad Request', { status: 400 });
	const reader = request.body.getReader();
	const remoteConnWrapper = { socket: null, connectingPromise: null, retryConnect: null };
	let isDnsQuery = false;
	const 木马UDP上下文 = { 缓存: new Uint8Array(0) };
	let 判断是否是木马 = null;
	let 当前写入Socket = null;
	let 远端写入器 = null;
	//log('[gRPC] 开始处理双向流');
	const grpcHeaders = new Headers({
		'Content-Type': 'application/grpc',
		'grpc-status': '0',
		'X-Accel-Buffering': 'no',
		'Cache-Control': 'no-store'
	});

	const 下行缓存上限 = 64 * 1024;
	const 下行刷新间隔 = 20;

	return new Response(new ReadableStream({
		async start(controller) {
			let 已关闭 = false;
			let 发送队列 = [];
			let 队列字节数 = 0;
			let 刷新定时器 = null;
			const grpcBridge = {
				readyState: WebSocket.OPEN,
				send(data) {
					if (已关闭) return;
					const chunk = data instanceof Uint8Array ? data : new Uint8Array(data);
					const lenBytes数组 = [];
					let remaining = chunk.byteLength >>> 0;
					while (remaining > 127) {
						lenBytes数组.push((remaining & 0x7f) | 0x80);
						remaining >>>= 7;
					}
					lenBytes数组.push(remaining);
					const lenBytes = new Uint8Array(lenBytes数组);
					const protobufLen = 1 + lenBytes.length + chunk.byteLength;
					const frame = new Uint8Array(5 + protobufLen);
					frame[0] = 0;
					frame[1] = (protobufLen >>> 24) & 0xff;
					frame[2] = (protobufLen >>> 16) & 0xff;
					frame[3] = (protobufLen >>> 8) & 0xff;
					frame[4] = protobufLen & 0xff;
					frame[5] = 0x0a;
					frame.set(lenBytes, 6);
					frame.set(chunk, 6 + lenBytes.length);
					发送队列.push(frame);
					队列字节数 += frame.byteLength;
					if (队列字节数 >= 下行缓存上限) 刷新发送队列();
					else if (!刷新定时器) 刷新定时器 = setTimeout(刷新发送队列, 下行刷新间隔);
				},
				close() {
					if (this.readyState === WebSocket.CLOSED) return;
					刷新发送队列(true);
					已关闭 = true;
					this.readyState = WebSocket.CLOSED;
					try { controller.close() } catch (e) { }
				}
			};

			const 刷新发送队列 = (force = false) => {
				if (刷新定时器) {
					clearTimeout(刷新定时器);
					刷新定时器 = null;
				}
				if ((!force && 已关闭) || 队列字节数 === 0) return;
				const out = new Uint8Array(队列字节数);
				let offset = 0;
				for (const item of 发送队列) {
					out.set(item, offset);
					offset += item.byteLength;
				}
				发送队列 = [];
				队列字节数 = 0;
				try {
					controller.enqueue(out);
				} catch (e) {
					已关闭 = true;
					grpcBridge.readyState = WebSocket.CLOSED;
				}
			};

			const 关闭连接 = () => {
				if (已关闭) return;
				刷新发送队列(true);
				已关闭 = true;
				grpcBridge.readyState = WebSocket.CLOSED;
				if (刷新定时器) clearTimeout(刷新定时器);
				if (远端写入器) {
					try { 远端写入器.releaseLock() } catch (e) { }
					远端写入器 = null;
				}
				当前写入Socket = null;
				try { reader.releaseLock() } catch (e) { }
				try { remoteConnWrapper.socket?.close() } catch (e) { }
				try { controller.close() } catch (e) { }
			};

			const 释放远端写入器 = () => {
				if (远端写入器) {
					try { 远端写入器.releaseLock() } catch (e) { }
					远端写入器 = null;
				}
				当前写入Socket = null;
			};

			const 写入远端 = async (payload, allowRetry = true) => {
				const socket = remoteConnWrapper.socket;
				if (!socket) return false;
				if (socket !== 当前写入Socket) {
					释放远端写入器();
					当前写入Socket = socket;
					远端写入器 = socket.writable.getWriter();
				}
				try {
					await 远端写入器.write(payload);
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
				let pending = new Uint8Array(0);
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					if (!value || value.byteLength === 0) continue;
					const 当前块 = value instanceof Uint8Array ? value : new Uint8Array(value);
					const merged = new Uint8Array(pending.length + 当前块.length);
					merged.set(pending, 0);
					merged.set(当前块, pending.length);
					pending = merged;
					while (pending.byteLength >= 5) {
						const grpcLen = ((pending[1] << 24) >>> 0) | (pending[2] << 16) | (pending[3] << 8) | pending[4];
						const frameSize = 5 + grpcLen;
						if (pending.byteLength < frameSize) break;
						const grpcPayload = pending.slice(5, frameSize);
						pending = pending.slice(frameSize);
						if (!grpcPayload.byteLength) continue;
						let payload = grpcPayload;
						if (payload.byteLength >= 2 && payload[0] === 0x0a) {
							let shift = 0;
							let offset = 1;
							let varint有效 = false;
							while (offset < payload.length) {
								const current = payload[offset++];
								if ((current & 0x80) === 0) {
									varint有效 = true;
									break;
								}
								shift += 7;
								if (shift > 35) break;
							}
							if (varint有效) payload = payload.slice(offset);
						}
						if (!payload.byteLength) continue;
						if (isDnsQuery) {
							if (判断是否是木马) await 转发木马UDP数据(payload, grpcBridge, 木马UDP上下文);
							else await forwardataudp(payload, grpcBridge, null);
							continue;
						}
						if (remoteConnWrapper.socket) {
							if (!(await 写入远端(payload))) throw new Error('Remote socket is not ready');
						} else {
							let 首包buffer;
							if (payload instanceof ArrayBuffer) 首包buffer = payload;
							else if (ArrayBuffer.isView(payload)) 首包buffer = payload.buffer.slice(payload.byteOffset, payload.byteOffset + payload.byteLength);
							else 首包buffer = new Uint8Array(payload).buffer;
							const 首包bytes = new Uint8Array(首包buffer);
							if (判断是否是木马 === null) 判断是否是木马 = 首包bytes.byteLength >= 58 && 首包bytes[56] === 0x0d && 首包bytes[57] === 0x0a;
							if (判断是否是木马) {
								const 解析结果 = 解析木马请求(首包buffer, yourUUID);
								if (解析结果?.hasError) throw new Error(解析结果.message || 'Invalid trojan request');
								const { port, hostname, rawClientData, isUDP } = 解析结果;
								log(`[gRPC] 木马首包: ${hostname}:${port} | UDP: ${isUDP ? '是' : '否'}`);
								if (isSpeedTestSite(hostname)) throw new Error('Speedtest site is blocked');
								if (isUDP) {
									isDnsQuery = true;
									if (有效数据长度(rawClientData) > 0) await 转发木马UDP数据(rawClientData, grpcBridge, 木马UDP上下文);
								} else {
									await forwardataTCP(hostname, port, rawClientData, grpcBridge, null, remoteConnWrapper, yourUUID);
								}
							} else {
								判断是否是木马 = false;
								const 解析结果 = 解析魏烈思请求(首包buffer, yourUUID);
								if (解析结果?.hasError) throw new Error(解析结果.message || 'Invalid 魏烈思 request');
								const { port, hostname, rawIndex, version, isUDP } = 解析结果;
								log(`[gRPC] 魏烈思首包: ${hostname}:${port} | UDP: ${isUDP ? '是' : '否'}`);
								if (isSpeedTestSite(hostname)) throw new Error('Speedtest site is blocked');
								if (isUDP) {
									if (port !== 53) throw new Error('UDP is not supported');
									isDnsQuery = true;
								}
								const respHeader = new Uint8Array([version[0], 0]);
								grpcBridge.send(respHeader);
								const rawData = 首包buffer.slice(rawIndex);
								if (isDnsQuery) {
									if (判断是否是木马) await 转发木马UDP数据(rawData, grpcBridge, 木马UDP上下文);
									else await forwardataudp(rawData, grpcBridge, null);
								}
								else await forwardataTCP(hostname, port, rawData, grpcBridge, null, remoteConnWrapper, yourUUID);
							}
						}
					}
					刷新发送队列();
				}
			} catch (err) {
				log(`[gRPC转发] 处理失败: ${err?.message || err}`);
			} finally {
				释放远端写入器();
				关闭连接();
			}
		},
		cancel() {
			try { remoteConnWrapper.socket?.close() } catch (e) { }
			try { reader.releaseLock() } catch (e) { }
		}
	}), { status: 200, headers: grpcHeaders });
}

