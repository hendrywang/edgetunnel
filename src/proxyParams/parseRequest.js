// parseProxyParams: 旧 `反代参数获取(url, uuid)` 的重写版。
// 不再写全局 let，而是改写传入的 ctx (ProxyContext 实例)。
// 调用方需要预先构造 ctx，并把它传给所有下游函数。
//
// 原始版本最大的隐患：模块级 let 在 isolate 并发请求中被互相覆盖。
// 把状态收回到 ctx 后，请求间彻底隔离，并发安全。

import { base64SecretDecode } from '../utils/base64Secret.js';
import { 反代协议默认端口, 获取代理默认端口 } from './ports.js';
import { 获取SOCKS5账号 } from './parseSocks5.js';

export async function parseProxyParams(ctx, url, uuid) {
	const { searchParams } = url;
	const pathname = decodeURIComponent(url.pathname);
	const pathLower = pathname.toLowerCase();

	const 链式代理路径匹配 = pathname.match(/\/video\/(.+)$/i);
	if (链式代理路径匹配) {
		try {
			const 链式代理明文 = base64SecretDecode(链式代理路径匹配[1], uuid);
			const { type, ...链式代理地址 } = JSON.parse(链式代理明文);
			if (!type || !反代协议默认端口[String(type).toLowerCase()]) throw new Error('链式代理类型无效');
			if (!链式代理地址.hostname || !链式代理地址.port) throw new Error('链式代理地址缺少 hostname 或 port');
			ctx.我的SOCKS5账号 = '';
			ctx.反代IP = '链式代理';
			ctx.启用反代兜底 = false;
			ctx.启用SOCKS5全局反代 = true;
			ctx.启用SOCKS5反代 = String(type).toLowerCase();
			ctx.parsedSocks5Address = {
				username: 链式代理地址.username,
				password: 链式代理地址.password,
				hostname: 链式代理地址.hostname,
				port: Number(链式代理地址.port)
			};
			if (isNaN(ctx.parsedSocks5Address.port)) throw new Error('链式代理端口无效');
			return;
		} catch (err) {
			console.error('解析链式代理参数失败:', err.message);
		}
	}

	ctx.我的SOCKS5账号 = searchParams.get('socks5') || searchParams.get('http') || searchParams.get('https') || searchParams.get('turn') || searchParams.get('sstp') || null;
	ctx.启用SOCKS5全局反代 = searchParams.has('globalproxy');
	if (searchParams.get('socks5')) ctx.启用SOCKS5反代 = 'socks5';
	else if (searchParams.get('http')) ctx.启用SOCKS5反代 = 'http';
	else if (searchParams.get('https')) ctx.启用SOCKS5反代 = 'https';
	else if (searchParams.get('turn')) ctx.启用SOCKS5反代 = 'turn';
	else if (searchParams.get('sstp')) ctx.启用SOCKS5反代 = 'sstp';

	const 解析代理URL = (值, 强制全局 = true) => {
		const 匹配 = /^(socks5|http|https|turn|sstp):\/\/(.+)$/i.exec(值 || '');
		if (!匹配) return false;
		ctx.启用SOCKS5反代 = 匹配[1].toLowerCase();
		ctx.我的SOCKS5账号 = 匹配[2].split('/')[0];
		if (强制全局) ctx.启用SOCKS5全局反代 = true;
		return true;
	};

	const 设置反代IP = (值) => {
		ctx.反代IP = 值;
		ctx.启用SOCKS5反代 = null;
		ctx.启用反代兜底 = false;
	};

	const 提取路径值 = (值) => {
		if (!值.includes('://')) {
			const 斜杠索引 = 值.indexOf('/');
			return 斜杠索引 > 0 ? 值.slice(0, 斜杠索引) : 值;
		}
		const 协议拆分 = 值.split('://');
		if (协议拆分.length !== 2) return 值;
		const 斜杠索引 = 协议拆分[1].indexOf('/');
		return 斜杠索引 > 0 ? `${协议拆分[0]}://${协议拆分[1].slice(0, 斜杠索引)}` : 值;
	};

	const 查询反代IP = searchParams.get('proxyip');
	if (查询反代IP !== null) {
		if (!解析代理URL(查询反代IP)) return 设置反代IP(查询反代IP);
	} else {
		let 匹配 = /\/(socks5?|http|https|turn|sstp):\/?\/?([^/?#\s]+)/i.exec(pathname);
		if (匹配) {
			const 类型 = 匹配[1].toLowerCase();
			ctx.启用SOCKS5反代 = 类型 === 'sock' || 类型 === 'socks' ? 'socks5' : 类型;
			ctx.我的SOCKS5账号 = 匹配[2].split('/')[0];
			ctx.启用SOCKS5全局反代 = true;
		} else if ((匹配 = /\/(g?s5|socks5|g?http|g?https|g?turn|g?sstp)=([^/?#\s]+)/i.exec(pathname))) {
			const 类型 = 匹配[1].toLowerCase();
			ctx.我的SOCKS5账号 = 匹配[2].split('/')[0];
			ctx.启用SOCKS5反代 = 类型.includes('sstp') ? 'sstp' : (类型.includes('turn') ? 'turn' : (类型.includes('https') ? 'https' : (类型.includes('http') ? 'http' : 'socks5')));
			if (类型.startsWith('g')) ctx.启用SOCKS5全局反代 = true;
		} else if ((匹配 = /\/(proxyip[.=]|pyip=|ip=)([^?#\s]+)/.exec(pathLower))) {
			const 路径反代值 = 提取路径值(匹配[2]);
			if (!解析代理URL(路径反代值)) return 设置反代IP(路径反代值);
		}
	}

	if (!ctx.我的SOCKS5账号) {
		ctx.启用SOCKS5反代 = null;
		return;
	}

	try {
		ctx.parsedSocks5Address = await 获取SOCKS5账号(ctx.我的SOCKS5账号, 获取代理默认端口(ctx.启用SOCKS5反代));
		if (searchParams.get('socks5')) ctx.启用SOCKS5反代 = 'socks5';
		else if (searchParams.get('http')) ctx.启用SOCKS5反代 = 'http';
		else if (searchParams.get('https')) ctx.启用SOCKS5反代 = 'https';
		else if (searchParams.get('turn')) ctx.启用SOCKS5反代 = 'turn';
		else if (searchParams.get('sstp')) ctx.启用SOCKS5反代 = 'sstp';
		else ctx.启用SOCKS5反代 = ctx.启用SOCKS5反代 || 'socks5';
	} catch (err) {
		console.error('解析SOCKS5地址失败:', err.message);
		ctx.启用SOCKS5反代 = null;
	}
}
