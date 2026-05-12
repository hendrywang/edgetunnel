// 调试日志开关。fetch 入口在阶段0时仍写 _worker.js 中的同名全局，
// 这里通过 setDebug() 暴露给入口去同步，后续阶段1引入 ProxyContext 后会替换。
let _debug = false;

export function setDebug(value) {
	_debug = !!value;
}

export function getDebug() {
	return _debug;
}

export function log(...args) {
	if (_debug) console.log(...args);
}
