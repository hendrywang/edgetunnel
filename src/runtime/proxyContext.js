// ProxyContext: 每一次 fetch 调用都创建一个独立实例，避免模块级全局
// 在 Cloudflare Workers isolate 中被并发请求互相覆盖。
//
// 旧代码用的模块级 let（反代IP / 启用SOCKS5反代 / 启用SOCKS5全局反代 /
// 我的SOCKS5账号 / parsedSocks5Address / 启用反代兜底 / SOCKS5白名单 /
// config_JSON）全部挪到这个对象里，由 fetch 入口构造并向下穿透。

const DEFAULT_SOCKS5_WHITELIST = [
	'*tapecontent.net',
	'*cloudatacdn.com',
	'*loadshare.org',
	'*cdn-centaurus.com',
	'scholar.google.com',
];

export function createProxyContext() {
	return {
		// 代理选择
		反代IP: '',
		启用SOCKS5反代: null,           // 'socks5' | 'http' | 'https' | 'turn' | 'sstp' | null
		启用SOCKS5全局反代: false,
		我的SOCKS5账号: '',
		parsedSocks5Address: {},        // { username, password, hostname, port }
		启用反代兜底: true,

		// 白名单（受 env.GO2SOCKS5 影响）
		SOCKS5白名单: [...DEFAULT_SOCKS5_WHITELIST],

		// 本次请求拼出的 config（由 router 调用 读取config_JSON 后赋值）
		config_JSON: null,
	};
}

export { DEFAULT_SOCKS5_WHITELIST };
