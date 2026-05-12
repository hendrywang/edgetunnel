export const 反代协议默认端口 = { socks5: 1080, http: 80, https: 443, turn: 3478, sstp: 443 };

export function 获取代理默认端口(类型) {
	return 反代协议默认端口[String(类型 || '').toLowerCase()] || 80;
}
