import { 整理成数组 } from './normalize.js';

export async function 获取优选订阅生成器数据(优选订阅生成器HOST) {
	let 优选IP = [], 其他节点LINK = '', 格式化HOST = 优选订阅生成器HOST.replace(/^sub:\/\//i, 'https://').split('#')[0].split('?')[0];
	if (!/^https?:\/\//i.test(格式化HOST)) 格式化HOST = `https://${格式化HOST}`;

	try {
		const url = new URL(格式化HOST);
		格式化HOST = url.origin;
	} catch (error) {
		优选IP.push(`127.0.0.1:1234#${优选订阅生成器HOST}优选订阅生成器格式化异常:${error.message}`);
		return [优选IP, 其他节点LINK];
	}

	const 优选订阅生成器URL = `${格式化HOST}/sub?host=example.com&uuid=00000000-0000-4000-8000-000000000000`;

	try {
		const response = await fetch(优选订阅生成器URL, {
			headers: { 'User-Agent': 'v2rayN/edge' + 'tunnel (https://github.com/cmliu/edge' + 'tunnel)' }
		});

		if (!response.ok) {
			优选IP.push(`127.0.0.1:1234#${优选订阅生成器HOST}优选订阅生成器异常:${response.statusText}`);
			return [优选IP, 其他节点LINK];
		}

		const 优选订阅生成器返回订阅内容 = atob(await response.text());
		const 订阅行列表 = 优选订阅生成器返回订阅内容.includes('\r\n')
			? 优选订阅生成器返回订阅内容.split('\r\n')
			: 优选订阅生成器返回订阅内容.split('\n');

		for (const 行内容 of 订阅行列表) {
			if (!行内容.trim()) continue; // 跳过空行
			if (行内容.includes('00000000-0000-4000-8000-000000000000') && 行内容.includes('example.com')) {
				// 这是优选IP行，提取 域名:端口#备注
				const 地址匹配 = 行内容.match(/:\/\/[^@]+@([^?]+)/);
				if (地址匹配) {
					let 地址端口 = 地址匹配[1], 备注 = ''; // 域名:端口 或 IP:端口
					const 备注匹配 = 行内容.match(/#(.+)$/);
					if (备注匹配) 备注 = '#' + decodeURIComponent(备注匹配[1]);
					优选IP.push(地址端口 + 备注);
				}
			} else {
				其他节点LINK += 行内容 + '\n';
			}
		}
	} catch (error) {
		优选IP.push(`127.0.0.1:1234#${优选订阅生成器HOST}优选订阅生成器异常:${error.message}`);
	}

	return [优选IP, 其他节点LINK];
}

