// TCP 转发主流程：先尝试直连 → 失败回退到反代池 / 上游代理（SOCKS5/HTTP/HTTPS/TURN/SSTP）。
// 全部代理参数从 ProxyContext 读，杜绝模块级 let 全局。
import { connect } from 'cloudflare:sockets';
import { 有效数据长度, 数据转Uint8Array } from '../utils/bytes.js';
import { log } from '../runtime/log.js';
import { closeSocketQuietly } from './socketUtils.js';
import { connectStreams } from './connectStreams.js';
import { 解析地址端口, getCacheIndex, setCacheIndex } from '../outbound/resolveProxyIp.js';
import { socks5Connect } from '../outbound/socks5.js';
import { httpConnect } from '../outbound/http.js';
import { httpsConnect } from '../outbound/https.js';
import { turnConnect } from '../outbound/turn.js';
import { sstpConnect } from '../outbound/sstp.js';
import { isIPHostname } from '../outbound/hostUtils.js';

export async function forwardataTCP(ctx, host, portNum, rawData, ws, respHeader, remoteConnWrapper, yourUUID) {
	log(`[TCP转发] 目标: ${host}:${portNum} | 反代IP: ${ctx.反代IP} | 反代兜底: ${ctx.启用反代兜底 ? '是' : '否'} | 反代类型: ${ctx.启用SOCKS5反代 || 'proxyip'} | 全局: ${ctx.启用SOCKS5全局反代 ? '是' : '否'}`);
	const 连接超时毫秒 = 1000;
	let 已通过代理发送首包 = false;

	async function 等待连接建立(remoteSock, timeoutMs = 连接超时毫秒) {
		await Promise.race([
			remoteSock.opened,
			new Promise((_, reject) => setTimeout(() => reject(new Error('连接超时')), timeoutMs))
		]);
	}

	async function connectDirect(address, port, data = null, 所有反代数组 = null, 反代兜底 = true) {
		let remoteSock;
		if (所有反代数组 && 所有反代数组.length > 0) {
			for (let i = 0; i < 所有反代数组.length; i++) {
				const 反代数组索引 = (getCacheIndex() + i) % 所有反代数组.length;
				const [反代地址, 反代端口] = 所有反代数组[反代数组索引];
				try {
					log(`[反代连接] 尝试连接到: ${反代地址}:${反代端口} (索引: ${反代数组索引})`);
					remoteSock = connect({ hostname: 反代地址, port: 反代端口 });
					await 等待连接建立(remoteSock);
					if (有效数据长度(data) > 0) {
						const testWriter = remoteSock.writable.getWriter();
						await testWriter.write(data);
						testWriter.releaseLock();
					}
					log(`[反代连接] 成功连接到: ${反代地址}:${反代端口}`);
					setCacheIndex(反代数组索引);
					return remoteSock;
				} catch (err) {
					log(`[反代连接] 连接失败: ${反代地址}:${反代端口}, 错误: ${err.message}`);
					try { remoteSock?.close?.() } catch (e) { }
					continue;
				}
			}
		}

		if (反代兜底) {
			remoteSock = connect({ hostname: address, port: port });
			await 等待连接建立(remoteSock);
			if (有效数据长度(data) > 0) {
				const writer = remoteSock.writable.getWriter();
				await writer.write(data);
				writer.releaseLock();
			}
			return remoteSock;
		} else {
			closeSocketQuietly(ws);
			throw new Error('[反代连接] 所有反代连接失败，且未启用反代兜底，连接终止。');
		}
	}

	async function connecttoPry(允许发送首包 = true) {
		if (remoteConnWrapper.connectingPromise) {
			await remoteConnWrapper.connectingPromise;
			return;
		}

		const 本次发送首包 = 允许发送首包 && !已通过代理发送首包 && 有效数据长度(rawData) > 0;
		const 本次首包数据 = 本次发送首包 ? rawData : null;

		const 当前连接任务 = (async () => {
			let newSocket;
			if (ctx.启用SOCKS5反代 === 'socks5') {
				log(`[SOCKS5代理] 代理到: ${host}:${portNum}`);
				newSocket = await socks5Connect(ctx, host, portNum, 本次首包数据);
			} else if (ctx.启用SOCKS5反代 === 'http') {
				log(`[HTTP代理] 代理到: ${host}:${portNum}`);
				newSocket = await httpConnect(ctx, host, portNum, 本次首包数据);
			} else if (ctx.启用SOCKS5反代 === 'https') {
				log(`[HTTPS代理] 代理到: ${host}:${portNum}`);
				newSocket = isIPHostname(ctx.parsedSocks5Address.hostname)
					? await httpsConnect(ctx, host, portNum, 本次首包数据)
					: await httpConnect(ctx, host, portNum, 本次首包数据, true);
			} else if (ctx.启用SOCKS5反代 === 'turn') {
				log(`[TURN代理] 代理到: ${host}:${portNum}`);
				newSocket = await turnConnect(ctx.parsedSocks5Address, host, portNum);
				if (有效数据长度(本次首包数据) > 0) {
					const writer = newSocket.writable.getWriter();
					try { await writer.write(数据转Uint8Array(本次首包数据)) }
					finally { try { writer.releaseLock() } catch (e) { } }
				}
			} else if (ctx.启用SOCKS5反代 === 'sstp') {
				log(`[SSTP代理] 代理到: ${host}:${portNum}`);
				newSocket = await sstpConnect(ctx.parsedSocks5Address, host, portNum);
				if (有效数据长度(本次首包数据) > 0) {
					const writer = newSocket.writable.getWriter();
					try { await writer.write(数据转Uint8Array(本次首包数据)) }
					finally { try { writer.releaseLock() } catch (e) { } }
				}
			} else {
				log(`[反代连接] 代理到: ${host}:${portNum}`);
				const 所有反代数组 = await 解析地址端口(ctx.反代IP, host, yourUUID);
				newSocket = await connectDirect(atob('UFJPWFlJUC50cDEuMDkwMjI3Lnh5eg=='), 1, 本次首包数据, 所有反代数组, ctx.启用反代兜底);
			}
			if (本次发送首包) 已通过代理发送首包 = true;
			remoteConnWrapper.socket = newSocket;
			newSocket.closed.catch(() => { }).finally(() => closeSocketQuietly(ws));
			connectStreams(newSocket, ws, respHeader, null);
		})();

		remoteConnWrapper.connectingPromise = 当前连接任务;
		try {
			await 当前连接任务;
		} finally {
			if (remoteConnWrapper.connectingPromise === 当前连接任务) {
				remoteConnWrapper.connectingPromise = null;
			}
		}
	}
	remoteConnWrapper.retryConnect = async () => connecttoPry(!已通过代理发送首包);

	if (ctx.启用SOCKS5反代 && (ctx.启用SOCKS5全局反代 || ctx.SOCKS5白名单.some(p => new RegExp(`^${p.replace(/\*/g, '.*')}$`, 'i').test(host)))) {
		log(`[TCP转发] 启用 SOCKS5/HTTP/HTTPS/TURN/SSTP 全局代理`);
		try {
			await connecttoPry();
		} catch (err) {
			log(`[TCP转发] SOCKS5/HTTP/HTTPS/TURN/SSTP 代理连接失败: ${err.message}`);
			throw err;
		}
	} else {
		try {
			log(`[TCP转发] 尝试直连到: ${host}:${portNum}`);
			const initialSocket = await connectDirect(host, portNum, rawData);
			remoteConnWrapper.socket = initialSocket;
			connectStreams(initialSocket, ws, respHeader, async () => {
				if (remoteConnWrapper.socket !== initialSocket) return;
				await connecttoPry();
			});
		} catch (err) {
			log(`[TCP转发] 直连 ${host}:${portNum} 失败: ${err.message}`);
			await connecttoPry();
		}
	}
}
