import { handleRequest } from './src/router/index.js';

export default {
	async fetch(request, env, cfCtx) {
		return handleRequest(request, env, cfCtx);
	}
};
