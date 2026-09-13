import { PassThrough } from "stream";
import path from 'path';
import _ from 'lodash';
import mime from 'mime';
import axios, { AxiosResponse } from 'axios';

import APIException from "@/lib/exceptions/APIException.ts";
import EX from "@/api/consts/exceptions.ts";
import logger from '@/lib/logger.ts';
import util from '@/lib/util.ts';

// 模型名称
const MODEL_NAME = 'kimi';
// access_token有效期
const ACCESS_TOKEN_EXPIRES = 300;
// 最大重试次数
const MAX_RETRY_COUNT = 3;
// 重试延迟
const RETRY_DELAY = 5000;
// 伪装headers
const FAKE_HEADERS = {
  'Accept': '*/*',
  'Accept-Encoding': 'gzip, deflate, br, zstd',
  'Accept-Language': 'zh-CN,zh;q=0.9',
  'Origin': 'https://kimi.moonshot.cn',
  'Cookie': util.generateCookie(),
  'R-Timezone': 'Asia/Shanghai',
  'Sec-Ch-Ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
  'Sec-Ch-Ua-Mobile': '?0',
  'Sec-Ch-Ua-Platform': '"Windows"',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
};
// 文件最大大小
const FILE_MAX_SIZE = 100 * 1024 * 1024;
// 文件上传重试次数与延迟（上游对免费用户有资源限制）
const UPLOAD_RETRY_COUNT = 2;
const UPLOAD_RETRY_DELAY = 3000;
// 上传结果短时缓存，避免请求重试时重复上传
const uploadedFileCache = new Map<string, { file: any, expireAt: number }>();
const UPLOAD_CACHE_TTL = 5 * 60 * 1000;
// 服务基地址
const KIMI_BASE_URL = 'https://kimi.moonshot.cn';
// 文件服务基地址（apiv2-files 仅部署在 www.kimi.com，kimi.moonshot.cn 会302重定向）
const KIMI_FILE_BASE_URL = 'https://www.kimi.com';
// 新版对话接口路径（Connect RPC）
const KIMI_CHAT_PATH = '/apiv2/kimi.gateway.chat.v1.ChatService/Chat';
// 新版删除会话接口路径（普通JSON RPC，命名空间不含gateway）
const KIMI_DELETE_CHAT_PATH = '/apiv2/kimi.chat.v1.ChatService/DeleteChat';
// 对话场景标识
const KIMI_SCENARIO = 'SCENARIO_K2D5';
// 思考阶段名称
const THINKING_STAGE_NAME = 'STAGE_NAME_THINKING';
// 思考语言提示，使思考过程语言与用户最新消息保持一致
const THINKING_LANGUAGE_HINT = '请使用与用户最新消息相同的语言输出思考过程和回答（若用户使用中文提问，请使用中文思考）。';
// 伪装Web客户端的设备与会话标识
const KIMI_DEVICE_ID = `7${util.generateRandomString({ length: 18, charset: 'numeric' })}`;
const KIMI_SESSION_ID = `17${util.generateRandomString({ length: 17, charset: 'numeric' })}`;
// access_token映射
const accessTokenMap = new Map();
// access_token请求队列映射
const accessTokenRequestQueueMap: Record<string, Function[]> = {};

/**
 * 请求access_token
 * 
 * 使用refresh_token去刷新获得access_token
 * 
 * @param refreshToken 用于刷新access_token的refresh_token
 */
async function requestToken(refreshToken: string) {
  if (accessTokenRequestQueueMap[refreshToken])
    return new Promise(resolve => accessTokenRequestQueueMap[refreshToken].push(resolve));
  accessTokenRequestQueueMap[refreshToken] = [];
  logger.info(`Refresh token: ${refreshToken}`);
  const result = await (async () => {
    const result = await axios.get('https://kimi.moonshot.cn/api/auth/token/refresh', {
      headers: {
        Accept: '*/*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        Authorization: `Bearer ${refreshToken}`,
        'Cache-Control': 'no-cache',
        'Cookie': util.generateCookie(),
        Pragma: 'no-cache',
        Referer: 'https://kimi.moonshot.cn/',
        'Sec-Ch-Ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      },
      timeout: 15000,
      validateStatus: () => true
    });
    const {
      access_token,
      refresh_token
    } = checkResult(result, refreshToken);
    const { id: userId } = await getUserInfo(access_token, refreshToken);
    return {
      userId,
      accessToken: access_token,
      refreshToken: refresh_token,
      refreshTime: util.unixTimestamp() + ACCESS_TOKEN_EXPIRES
    }
  })()
    .then(result => {
      if (accessTokenRequestQueueMap[refreshToken]) {
        accessTokenRequestQueueMap[refreshToken].forEach(resolve => resolve(result));
        delete accessTokenRequestQueueMap[refreshToken];
      }
      logger.success(`Refresh successful`);
      return result;
    })
    .catch(err => {
      if (accessTokenRequestQueueMap[refreshToken]) {
        accessTokenRequestQueueMap[refreshToken].forEach(resolve => resolve(err));
        delete accessTokenRequestQueueMap[refreshToken];
      }
      return err;
    });
  if (_.isError(result))
    throw result;
  return result;
}

/**
 * 获取缓存中的access_token
 * 
 * 避免短时间大量刷新token，未加锁，如果有并发要求还需加锁
 * 
 * @param refreshToken 用于刷新access_token的refresh_token
 */
async function acquireToken(refreshToken: string): Promise<any> {
  let result = accessTokenMap.get(refreshToken);
  if (!result) {
    result = await requestToken(refreshToken);
    accessTokenMap.set(refreshToken, result);
  }
  if (util.unixTimestamp() > result.refreshTime) {
    result = await requestToken(refreshToken);
    accessTokenMap.set(refreshToken, result);
  }
  return result;
}

/**
 * 获取用户信息
 * 
 * @param refreshToken 用于刷新access_token的refresh_token
 */
async function getUserInfo(accessToken: string, refreshToken: string) {
  const result = await axios.get('https://kimi.moonshot.cn/api/user', {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Referer: 'https://kimi.moonshot.cn/',
      'X-Traffic-Id': `7${util.generateRandomString({ length: 18, charset: 'numeric' })}`,
      ...FAKE_HEADERS
    },
    timeout: 15000,
    validateStatus: () => true
  });
  return checkResult(result, refreshToken);
}

/**
 * 会话上下文
 */
interface ConversationContext {
  /** 对外暴露的会话ID */
  requestConversationId: string;
  /** 上游远端会话ID */
  remoteChatId?: string;
  /** 上游最后一条assistant消息ID */
  lastAssistantMessageId?: string;
}

// 会话上下文映射，用于多轮对话续聊
const conversationContextMap = new Map<string, ConversationContext>();

/**
 * 生成Connect协议请求体
 * 
 * Connect协议帧格式：第1字节为压缩标志(0x00表示未压缩)，
 * 随后4字节为大端序的JSON长度，最后为JSON内容
 * 
 * @param payload 请求载荷
 */
function encodeConnectRequest(payload: any): Buffer {
  const body = Buffer.from(JSON.stringify(payload), 'utf-8');
  const header = Buffer.alloc(5);
  header[0] = 0x00;
  header.writeUInt32BE(body.length, 1);
  return Buffer.concat([header, body]);
}

/**
 * 构建通用请求头
 * 
 * @param accessToken 访问令牌
 * @param userId 用户ID
 * @param baseUrl 服务基地址
 */
function buildBaseHeaders(accessToken: string, userId: string, baseUrl: string = KIMI_BASE_URL) {
  return {
    ...FAKE_HEADERS,
    Origin: baseUrl,
    Referer: `${baseUrl}/`,
    Authorization: `Bearer ${accessToken}`,
    'X-Traffic-Id': userId,
    'X-Msh-Platform': 'web',
    'X-Msh-Device-Id': KIMI_DEVICE_ID,
    'X-Msh-Session-Id': KIMI_SESSION_ID,
    'Priority': 'u=1, i'
  };
}

/**
 * 构建新版对话接口请求头
 * 
 * @param accessToken 访问令牌
 * @param userId 用户ID
 */
function buildChatHeaders(accessToken: string, userId: string) {
  return {
    ...buildBaseHeaders(accessToken, userId),
    'Connect-Protocol-Version': '1',
    'Content-Type': 'application/connect+json'
  };
}

/**
 * 解析/创建会话上下文
 * 
 * @param refConvId 引用的会话ID
 */
function resolveConversationContext(refConvId?: string): ConversationContext {
  const id = _.isString(refConvId) && refConvId.length ? refConvId : util.uuid();
  let context = conversationContextMap.get(id);
  if (!context) {
    context = { requestConversationId: id };
    conversationContextMap.set(id, context);
  }
  return context;
}

/**
 * 根据上游事件更新会话上下文
 * 
 * @param context 会话上下文
 * @param event 上游事件
 */
function updateContextFromEvent(context: ConversationContext, event: any) {
  if (_.isObject(event.chat) && _.isString(event.chat.id))
    context.remoteChatId = event.chat.id;
  if (_.isObject(event.message) && event.message.role === 'assistant' && _.isString(event.message.id))
    context.lastAssistantMessageId = event.message.id;
}

/**
 * 判断是否开启思考模式
 * 
 * 显式传参优先，其次模型名包含thinking开启，包含instant关闭，默认开启
 * 
 * @param model 模型名称
 * @param explicit 显式指定的开关
 */
function resolveThinkingEnabled(model: string, explicit?: any): boolean {
  if (_.isBoolean(explicit))
    return explicit;
  const name = _.isString(model) ? model.toLowerCase() : '';
  if (name.indexOf('instant') != -1 || name.indexOf('no_thinking') != -1)
    return false;
  return true;
}

/**
 * 构建新版对话接口载荷
 * 
 * @param content 合并后的消息内容
 * @param model 模型名称
 * @param useSearch 是否开启联网搜索
 * @param enableThinking 是否开启思考模式
 * @param context 会话上下文
 * @param fileRefs 引用文件列表（上传接口返回的文件对象）
 */
function buildChatPayload(content: string, model: string, useSearch: boolean, enableThinking: boolean, context: ConversationContext, fileRefs: any[] = []) {
  // 文件块在前，文本块在后
  const blocks: any[] = (fileRefs || []).map(file => ({
    message_id: '',
    file: _.isObject(file) && file.id ? { id: file.id } : file
  }));
  blocks.push({
    message_id: '',
    text: { content }
  });
  const message: any = {
    role: 'user',
    blocks,
    scenario: KIMI_SCENARIO
  };
  if (context.lastAssistantMessageId)
    message.parent_id = context.lastAssistantMessageId;

  const payload: any = {
    scenario: KIMI_SCENARIO,
    tools: useSearch ? [{ type: 'TOOL_TYPE_SEARCH', search: {} }] : [],
    message,
    options: {
      thinking: enableThinking
    }
  };
  // 20位智能体ID
  if (/^[0-9a-z]{20}$/.test(model))
    payload.kimiplusId = model;
  // 续聊时携带远端会话ID
  if (context.remoteChatId)
    payload.chat_id = context.remoteChatId;
  return payload;
}

/**
 * 从上游事件中解析显式阶段
 * 
 * @param event 上游事件
 */
function extractExplicitPhase(event: any): string | null {
  const stages = _.get(event, 'block.multiStage.stages');
  if (_.isArray(stages) && stages.length) {
    const stage = stages[0];
    if (_.isObject(stage) && stage.name === THINKING_STAGE_NAME)
      return stage.status === 'completed' ? 'answer' : 'thinking';
  }
  const flags = _.get(event, 'block.text.flags');
  if (flags === 'thinking')
    return 'thinking';
  if (flags === 'answer')
    return 'answer';
  return null;
}

/**
 * 从上游事件中提取增量内容
 * 
 * @param event 上游事件
 * @param currentPhase 当前阶段(thinking/answer)
 */
function extractDelta(event: any, currentPhase: string | null) {
  if (event.heartbeat)
    return { phase: currentPhase, content: null as string | null, reasoning: null as string | null };
  const explicitPhase = extractExplicitPhase(event);
  const phase = explicitPhase || currentPhase;
  const mask = _.isString(event.mask) ? event.mask : '';
  // 思考块
  if (mask.indexOf('block.think') != -1) {
    const content = _.get(event, 'block.think.content');
    return { phase: phase || 'thinking', content: null, reasoning: _.isString(content) ? content : null };
  }
  // 文本块（可能属于思考阶段）
  const textContent = _.get(event, 'block.text.content');
  if (mask.indexOf('block.text') != -1 || _.isString(textContent)) {
    if (explicitPhase === 'thinking')
      return { phase, content: null, reasoning: _.isString(textContent) ? textContent : null };
    return { phase: explicitPhase ? phase : 'answer', content: _.isString(textContent) ? textContent : null, reasoning: null };
  }
  return { phase, content: null, reasoning: null };
}

/**
 * 创建Connect协议流解析器
 * 
 * 按帧解析上游数据，跳过控制帧，解析出JSON事件
 * 
 * @param onEvent 事件回调
 * @param onError 错误回调
 */
function createConnectStreamParser(onEvent: (event: any) => void, onError: (err: any) => void) {
  let buffer = Buffer.alloc(0);
  let eventIndex = 0;
  return (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);
    let offset = 0;
    while (offset + 5 <= buffer.length) {
      const flag = buffer[offset];
      const length = buffer.readUInt32BE(offset + 1);
      const frameEnd = offset + 5 + length;
      if (frameEnd > buffer.length)
        break;
      const payload = buffer.subarray(offset + 5, frameEnd);
      offset = frameEnd;
      // 跳过控制帧
      if (flag & 0x80)
        continue;
      const text = payload.toString('utf-8').trim();
      if (!text)
        continue;
      const event = _.attempt(() => JSON.parse(text));
      if (_.isError(event))
        continue;
      // 上游错误（注意：上游可能先发送空done，随后再补发错误帧说明原因）
      if (_.isObject(event.error)) {
        const errInfo = event.error;
        const detail = errInfo.message
          || _.get(errInfo, 'details[0].debug.localizedMessage.message')
          || JSON.stringify(errInfo);
        logger.error(`上游返回错误: ${JSON.stringify(errInfo)}`);
        onError(new APIException(EX.API_REQUEST_FAILED, errInfo.code === 'resource_exhausted'
          ? '[上游限流] 当前免费账号被上游限流，请稍后重试'
          : `[请求kimi失败]: ${detail}`));
        return;
      }
      // 调试：记录前若干条事件，便于定位上游协议变化
      const mask = _.isString(event.mask) ? event.mask : '';
      if (!event.heartbeat && eventIndex < 12)
        logger.debug(`[connect#${eventIndex}] mask=${mask} ${text.length > 300 ? text.substring(0, 300) + '...' : text}`);
      eventIndex++;
      onEvent(event);
    }
    if (offset)
      buffer = buffer.subarray(offset);
  };
}

/**
 * 移除会话
 * 
 * 在对话流传输完毕后移除会话，避免创建的会话出现在用户的对话列表中
 * 注意：会话由新版接口创建，旧版 /api/chat/{id} 无法识别新版会话ID，
 * 因此必须使用新版 JSON 接口 kimi.chat.v1.ChatService/DeleteChat
 * 
 * @param convId 上游会话ID
 * @param refreshToken 用于刷新access_token的refresh_token
 */
async function removeConversation(convId: string, refreshToken: string) {
  const {
    accessToken,
    userId
  } = await acquireToken(refreshToken);
  const result = await axios.post(`${KIMI_BASE_URL}${KIMI_DELETE_CHAT_PATH}`, {
    chat_id: convId
  }, {
    headers: {
      ...buildBaseHeaders(accessToken, userId),
      'Connect-Protocol-Version': '1',
      'Content-Type': 'application/json'
    },
    timeout: 15000,
    validateStatus: () => true
  });
  // token失效交由checkResult处理并清理缓存token
  if (result.status == 401) {
    checkResult(result, refreshToken);
    return false;
  }
  return result.status == 200;
}

/**
 * prompt片段提交
 * 
 * @param query prompt
 * @param refreshToken 用于刷新access_token的refresh_token
 */
async function promptSnippetSubmit(query: string, refreshToken: string) {
  const {
    accessToken,
    userId
  } = await acquireToken(refreshToken);
  const result = await axios.post('https://kimi.moonshot.cn/api/prompt-snippet/instance', {
    "offset": 0,
    "size": 10,
    "query": query.replace('user:', '').replace('assistant:', '')
  }, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Referer: 'https://kimi.moonshot.cn/',
      'X-Traffic-Id': userId,
      ...FAKE_HEADERS
    },
    timeout: 15000,
    validateStatus: () => true
  });
  checkResult(result, refreshToken);
}

/**
 * 请求新版对话接口
 * 
 * 返回上游Connect协议流及会话上下文
 * 
 * @param model 模型名称
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 * @param refreshToken 用于刷新access_token的refresh_token
 * @param useSearch 是否开启联网搜索
 * @param refConvId 引用会话ID
 * @param enableThinking 是否开启思考模式
 */
async function requestChatCompletion(model: string, messages: any[], refreshToken: string, useSearch: boolean, refConvId?: string, enableThinking?: boolean) {
  // 提取引用文件URL并串行上传kimi（并发易触发上游限流）
  const refFileUrls = extractRefFileUrls(messages);
  const refs: any[] = [];
  for (const fileUrl of refFileUrls)
    refs.push(await uploadFile(fileUrl, refreshToken));

  // 伪装调用获取用户信息
  fakeRequest(refreshToken)
    .catch(err => logger.error(err));

  const {
    accessToken,
    userId
  } = await acquireToken(refreshToken);

  const context = resolveConversationContext(refConvId);
  const sendMessages = messagesPrepare(messages, !!refConvId);
  const content = sendMessages[0].content;
  const payload = buildChatPayload(content, model, useSearch, resolveThinkingEnabled(model, enableThinking), context, refs);
  logger.debug("新版对话接口载荷:", JSON.stringify(payload));

  const result = await axios.post(`${KIMI_BASE_URL}${KIMI_CHAT_PATH}`, encodeConnectRequest(payload), {
    headers: buildChatHeaders(accessToken, userId),
    // 120秒超时
    timeout: 120000,
    validateStatus: () => true,
    responseType: 'stream',
    maxBodyLength: Infinity,
    maxContentLength: Infinity
  });
  if (result.status == 401) {
    accessTokenMap.delete(refreshToken);
    throw new APIException(EX.API_TOKEN_EXPIRES, '[Token失效] 授权已过期，请重新从浏览器获取新的 refresh_token');
  }
  if (result.status != 200)
    throw new APIException(EX.API_REQUEST_FAILED, `[请求kimi失败]: ${result.status} ${result.statusText}`);
  return {
    stream: result.data,
    context,
    content
  };
}

/**
 * 同步对话补全
 * 
 * @param model 模型名称
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 * @param refreshToken 用于刷新access_token的refresh_token
 * @param useSearch 是否开启联网搜索
 * @param refConvId 引用会话ID
 * @param enableThinking 是否开启思考模式
 * @param retryCount 重试次数
 */
async function createCompletion(model = MODEL_NAME, messages: any[], refreshToken: string, useSearch = true, refConvId?: string, enableThinking?: boolean, retryCount = 0) {
  return (async () => {
    logger.info(messages);
    const {
      stream,
      context,
      content
    } = await requestChatCompletion(model, messages, refreshToken, useSearch, refConvId, enableThinking);

    const streamStartTime = util.timestamp();
    // 接收流为输出文本
    const answer = await receiveStream(model, context, stream);
    logger.success(`Stream has completed transfer ${util.timestamp() - streamStartTime}ms`);
    // 上游限流时可能只返回空内容，此处视为失败以触发重试
    const outputMessage = answer.choices[0].message;
    if (!outputMessage.content && !outputMessage.reasoning_content)
      throw new APIException(EX.API_REQUEST_FAILED, '[上游限流] 上游未返回任何内容，请稍后重试');

    // 异步移除会话，避免创建的会话出现在用户的对话列表中
    // 如果引用会话将不会清除，因为我们不知道什么时候你会结束会话
    !refConvId && context.remoteChatId && removeConversation(context.remoteChatId, refreshToken)
      .then(removed => removed || logger.warn(`移除会话失败（可忽略）: ${context.remoteChatId}`))
      .catch(err => logger.warn(`移除会话异常（可忽略）: ${err.message}`));
    promptSnippetSubmit(content, refreshToken)
      .catch(err => logger.warn(`prompt片段提交失败（可忽略）: ${err.message}`));

    return answer;
  })()
    .catch(err => {
      if (retryCount < MAX_RETRY_COUNT) {
        // 指数退避，避免重试加剧上游限流
        const delay = RETRY_DELAY * Math.pow(2, retryCount);
        logger.error(`Stream response error: ${err.message}`);
        logger.warn(`Try again after ${delay / 1000}s...`);
        return (async () => {
          await new Promise(resolve => setTimeout(resolve, delay));
          return createCompletion(model, messages, refreshToken, useSearch, refConvId, enableThinking, retryCount + 1);
        })();
      }
      throw err;
    });
}

/**
 * 流式对话补全
 * 
 * @param model 模型名称
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 * @param refreshToken 用于刷新access_token的refresh_token
 * @param useSearch 是否开启联网搜索
 * @param refConvId 引用会话ID
 * @param enableThinking 是否开启思考模式
 * @param retryCount 重试次数
 */
async function createCompletionStream(model = MODEL_NAME, messages: any[], refreshToken: string, useSearch = true, refConvId?: string, enableThinking?: boolean, retryCount = 0) {
  return (async () => {
    logger.info(messages);
    const {
      stream,
      context,
      content
    } = await requestChatCompletion(model, messages, refreshToken, useSearch, refConvId, enableThinking);

    const streamStartTime = util.timestamp();
    // 创建转换流将消息格式转换为gpt兼容格式
    return createTransStream(model, context, stream, {
      retryCount,
      endCallback: () => {
        logger.success(`Stream has completed transfer ${util.timestamp() - streamStartTime}ms`);
        // 流传输结束后异步移除会话
        // 如果引用会话将不会清除，因为我们不知道什么时候你会结束会话
        !refConvId && context.remoteChatId && removeConversation(context.remoteChatId, refreshToken)
          .then(removed => removed || logger.warn(`移除会话失败（可忽略）: ${context.remoteChatId}`))
          .catch(err => logger.warn(`移除会话异常（可忽略）: ${err.message}`));
        promptSnippetSubmit(content, refreshToken)
          .catch(err => logger.warn(`prompt片段提交失败（可忽略）: ${err.message}`));
      },
      // 首个输出前失败（如被上游限流）时自动换流重试
      retryCallback: () => createCompletionStream(model, messages, refreshToken, useSearch, refConvId, enableThinking, retryCount + 1)
    });
  })()
    .catch(err => {
      if (retryCount < MAX_RETRY_COUNT) {
        // 指数退避，避免重试加剧上游限流
        const delay = RETRY_DELAY * Math.pow(2, retryCount);
        logger.error(`Stream response error: ${err.message}`);
        logger.warn(`Try again after ${delay / 1000}s...`);
        return (async () => {
          await new Promise(resolve => setTimeout(resolve, delay));
          return createCompletionStream(model, messages, refreshToken, useSearch, refConvId, enableThinking, retryCount + 1);
        })();
      }
      throw err;
    });
}

/**
 * 调用一些接口伪装访问
 * 
 * 随机挑一个
 * 
 * @param refreshToken 用于刷新access_token的refresh_token
 */
async function fakeRequest(refreshToken: string) {
  const {
    accessToken,
    userId
  } = await acquireToken(refreshToken);
  const options = {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Referer: `https://kimi.moonshot.cn/`,
      'X-Traffic-Id': userId,
      ...FAKE_HEADERS
    }
  };
  await [
    () => axios.get('https://kimi.moonshot.cn/api/user', options),
    () => axios.get('https://kimi.moonshot.cn/api/chat_1m/user/status', options),
    () => axios.post('https://kimi.moonshot.cn/api/chat/list', {
      offset: 0,
      size: 50
    }, options),
    () => axios.post('https://kimi.moonshot.cn/api/show_case/list', {
      offset: 0,
      size: 4,
      enable_cache: true,
      order: "asc"
    }, options)
  ][Math.floor(Math.random() * 4)]();
}

/**
 * 提取消息中引用的文件URL
 * 
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 */
function extractRefFileUrls(messages: any[]) {
  const urls = [];
  // 如果没有消息，则返回[]
  if (!messages.length) {
    return urls;
  }
  // 只获取最新的消息
  const lastMessage = messages[messages.length - 1];
  if (_.isArray(lastMessage.content)) {
    lastMessage.content.forEach(v => {
      if (!_.isObject(v) || !['file', 'image_url'].includes(v['type']))
        return;
      // kimi-free-api支持格式
      if (v['type'] == 'file' && _.isObject(v['file_url']) && _.isString(v['file_url']['url']))
        urls.push(v['file_url']['url']);
      // 兼容gpt-4-vision-preview API格式
      else if (v['type'] == 'image_url' && _.isObject(v['image_url']) && _.isString(v['image_url']['url']))
        urls.push(v['image_url']['url']);
    });
  }
  logger.info("本次请求上传：" + urls.length + "个文件");
  return urls;
}

/**
 * 消息预处理
 * 
 * 由于接口只取第一条消息，此处会将多条消息合并为一条，实现多轮对话效果
 * user:旧消息1
 * assistant:旧消息2
 * user:新消息
 * 
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 * @param isRefConv 是否为引用会话
 */
function messagesPrepare(messages: any[], isRefConv = false) {
  let content;
  if (isRefConv || messages.length < 2) {
    content = messages.reduce((content, message) => {
      if (_.isArray(message.content)) {
        return message.content.reduce((_content, v) => {
          if (!_.isObject(v) || v['type'] != 'text') return _content;
          return _content + `${v["text"] || ""}\n`;
        }, content);
      }
      return content += `${message.content}\n`;
    }, '')
    logger.info("\n透传内容：\n" + content);
  }
  else {
    // 注入消息提升注意力
    let latestMessage = messages[messages.length - 1];
    let hasFileOrImage = Array.isArray(latestMessage.content)
      && latestMessage.content.some(v => (typeof v === 'object' && ['file', 'image_url'].includes(v['type'])));
    // 第二轮开始注入system prompt
    if (hasFileOrImage) {
      let newFileMessage = {
        "content": "关注用户最新发送文件和消息",
        "role": "system"
      };
      messages.splice(messages.length - 1, 0, newFileMessage);
      logger.info("注入提升尾部文件注意力system prompt");
    } else {
      let newTextMessage = {
        "content": "关注用户最新的消息",
        "role": "system"
      };
      messages.splice(messages.length - 1, 0, newTextMessage);
      logger.info("注入提升尾部消息注意力system prompt");
    }
    content = messages.reduce((content, message) => {
      if (_.isArray(message.content)) {
        return message.content.reduce((_content, v) => {
          if (!_.isObject(v) || v['type'] != 'text') return _content;
          return _content + `${message.role || "user"}:${v["text"] || ""}\n`;
        }, content);
      }
      return content += `${message.role || "user"}:${message.content}\n`;
    }, '')
    logger.info("\n对话合并：\n" + content);
  }

  // 注入思考语言提示，避免思考过程语言被上下文中的其他语言带偏
  content = `system:${THINKING_LANGUAGE_HINT}\n${content}`;

  return [
    { role: 'user', content }
  ]
}

/**
 * 预检查文件URL有效性
 * 
 * @param fileUrl 文件URL
 */
async function checkFileUrl(fileUrl: string) {
  if (util.isBASE64Data(fileUrl))
    return;
  const result = await axios.head(fileUrl, {
    timeout: 15000,
    validateStatus: () => true
  });
  if (result.status >= 400)
    throw new APIException(EX.API_FILE_URL_INVALID, `File ${fileUrl} is not valid: [${result.status}] ${result.statusText}`);
  // 检查文件大小
  if (result.headers && result.headers['content-length']) {
    const fileSize = parseInt(result.headers['content-length'], 10);
    if (fileSize > FILE_MAX_SIZE)
      throw new APIException(EX.API_FILE_EXECEEDS_SIZE, `File ${fileUrl} is not valid`);
  }
}

/**
 * 上传文件
 * 
 * 使用新版上传接口，返回上游文件对象（含id）
 * 
 * @param fileUrl 文件URL
 * @param refreshToken 用于刷新access_token的refresh_token
 */
async function uploadFile(fileUrl: string, refreshToken: string) {
  // 命中缓存则直接复用，避免重试重复上传
  const cacheKey = `${refreshToken.slice(0, 12)}|${fileUrl.slice(0, 120)}|${fileUrl.length}`;
  const cached = uploadedFileCache.get(cacheKey);
  if (cached && cached.expireAt > Date.now())
    return cached.file;

  // 预检查远程文件URL可用性
  await checkFileUrl(fileUrl);

  let filename, fileData, mimeType;
  // 如果是BASE64数据则直接转换为Buffer
  if (util.isBASE64Data(fileUrl)) {
    mimeType = util.extractBASE64DataFormat(fileUrl);
    const ext = mime.getExtension(mimeType);
    filename = `${util.uuid()}.${ext}`;
    fileData = Buffer.from(util.removeBASE64DataHeader(fileUrl), 'base64');
  }
  // 下载文件到内存，如果您的服务器内存很小，建议考虑改造为流直传到下一个接口上，避免停留占用内存
  else {
    filename = path.basename(fileUrl);
    ({ data: fileData } = await axios.get(fileUrl, {
      responseType: 'arraybuffer',
      // 100M限制
      maxContentLength: FILE_MAX_SIZE,
      // 60秒超时
      timeout: 60000
    }));
  }

  const {
    accessToken,
    userId
  } = await acquireToken(refreshToken);

  // 新版上传接口：multipart/form-data，字段名为file
  mimeType = mimeType || mime.getType(filename) || 'application/octet-stream';
  const form = new FormData();
  form.append('file', new Blob([fileData], { type: mimeType }), filename);
  let lastError;
  // 上游对免费用户有资源限制，失败时短暂重试
  for (let attempt = 0; attempt <= UPLOAD_RETRY_COUNT; attempt++) {
    if (attempt > 0) {
      logger.warn(`文件上传失败重试(${attempt}/${UPLOAD_RETRY_COUNT})...`);
      await new Promise(resolve => setTimeout(resolve, UPLOAD_RETRY_DELAY));
    }
    try {
      const result = await axios.post(`${KIMI_FILE_BASE_URL}/apiv2-files/file/upload`, form, {
        headers: buildBaseHeaders(accessToken, userId, KIMI_FILE_BASE_URL),
        // 120秒超时
        timeout: 120000,
        // 100M限制
        maxBodyLength: FILE_MAX_SIZE,
        maxContentLength: FILE_MAX_SIZE,
        validateStatus: () => true
      });
      // 记录上游响应，便于定位异常
      const bodyText = _.isString(result.data) ? result.data : JSON.stringify(result.data);
      logger.debug(`文件上传响应: status=${result.status} contentType=${_.get(result, 'headers.content-type')} body=${String(bodyText).slice(0, 800)}`);
      if (result.status != 200)
        logger.warn(`文件上传非200响应: status=${result.status} body=${String(bodyText).slice(0, 800)}`);
      const { file } = checkResult(result, refreshToken);
      if (_.isObject(file) && _.isString(file.id)) {
        logger.success(`文件上传成功: ${file.id}`);
        uploadedFileCache.set(cacheKey, { file, expireAt: Date.now() + UPLOAD_CACHE_TTL });
        return file;
      }
      lastError = new APIException(EX.API_REQUEST_FAILED, `[请求kimi失败]: 文件上传响应异常`);
    }
    catch (err) {
      lastError = err;
    }
  }
  throw lastError;
}

/**
 * 检查请求结果
 * 
 * @param result 结果
 * @param refreshToken 用于刷新access_token的refresh_token
 */
function checkResult(result: AxiosResponse, refreshToken: string) {
  if (result.status == 401) {
    accessTokenMap.delete(refreshToken);
    throw new APIException(EX.API_TOKEN_EXPIRES, '[Token失效] 授权已过期，请重新从浏览器获取新的 refresh_token');
  }
  if (!result.data)
    return null;
  const { error_type, message, code, details } = result.data;
  // 新版接口错误格式：{code, details:[{debug:{reason, localizedMessage:{message}}}]}
  if (_.isString(code)) {
    const detail: any = _.isArray(details) && details.length ? (details[0].debug || details[0]) : {};
    const reason = detail.reason || _.get(detail, 'localizedMessage.message') || '';
    throw new APIException(EX.API_REQUEST_FAILED, `[请求kimi失败]: ${code}${reason ? ` (${reason})` : ''}`);
  }
  if (!_.isString(error_type))
    return result.data;
  if (error_type == 'auth.token.invalid') {
    accessTokenMap.delete(refreshToken);
    throw new APIException(EX.API_TOKEN_EXPIRES, '[Token失效] 授权已过期，请重新从浏览器获取新的 refresh_token');
  }
  if (error_type == 'chat.user_stream_pushing')
    throw new APIException(EX.API_CHAT_STREAM_PUSHING);
  throw new APIException(EX.API_REQUEST_FAILED, `[请求kimi失败]: ${message}`);
}

/**
 * 移除文本中的非法替换字符
 * 
 * @param text 文本
 */
function trimInvalidChar(text: any) {
  if (!_.isString(text))
    return '';
  const exceptCharIndex = text.indexOf("�");
  return text.substring(0, exceptCharIndex == -1 ? text.length : exceptCharIndex);
}

/**
 * 从流接收完整的消息内容
 * 
 * @param model 模型名称
 * @param context 会话上下文
 * @param stream 消息流
 */
async function receiveStream(model: string, context: ConversationContext, stream: any) {
  return new Promise((resolve, reject) => {
    // 消息初始化
    const data = {
      id: context.requestConversationId,
      model,
      object: 'chat.completion',
      choices: [
        { index: 0, message: { role: 'assistant', content: '', reasoning_content: '' }, finish_reason: 'stop' }
      ],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      created: util.unixTimestamp()
    };
    let currentPhase: string | null = null;
    // 收尾，思考内容为空时不输出该字段，保持与原有响应结构兼容
    const finalize = () => {
      if (!data.choices[0].message.reasoning_content)
        delete data.choices[0].message.reasoning_content;
      return data;
    };
    const feed = createConnectStreamParser(
      event => {
        updateContextFromEvent(context, event);
        const delta = extractDelta(event, currentPhase);
        currentPhase = delta.phase || null;
        if (delta.reasoning)
          data.choices[0].message.reasoning_content += trimInvalidChar(delta.reasoning);
        if (delta.content)
          data.choices[0].message.content += trimInvalidChar(delta.content);
        // 上游可能先发送空done再补发错误帧，因此这里不立即结束，统一等流关闭后收尾
      },
      err => reject(err)
    );
    // 将流数据喂给Connect协议解析器
    stream.on("data", (chunk: Buffer) => feed(chunk));
    stream.once("error", (err: any) => reject(err));
    stream.once("close", () => resolve(finalize()));
  });
}

/**
 * 创建转换流
 * 
 * 将流格式转换为gpt兼容流格式
 * 
 * @param model 模型名称
 * @param context 会话上下文
 * @param stream 消息流
 * @param options 选项：endCallback 传输结束回调；retryCallback 首个输出前失败时的换流重试回调；retryCount 当前重试次数
 */
function createTransStream(model: string, context: ConversationContext, stream: any, options: { endCallback?: Function, retryCallback?: Function, retryCount?: number } = {}) {
  const { endCallback, retryCallback, retryCount = 0 } = options;
  // 消息创建时间
  const created = util.unixTimestamp();
  // 创建转换流
  const transStream = new PassThrough();
  let currentPhase: string | null = null;
  let finished = false;
  // 是否已产生有效输出（输出前失败可安全换流重试）
  let producedOutput = false;
  // 是否正在换流重试
  let retrying = false;
  // 写入一个gpt兼容的chunk
  const writeChunk = (delta: any, finishReason: string | null = null, usage?: any) => {
    const data = `data: ${JSON.stringify({
      id: context.requestConversationId,
      model,
      object: 'chat.completion.chunk',
      choices: [
        { index: 0, delta, finish_reason: finishReason }
      ],
      ...(usage ? { usage } : {}),
      created
    })}\n\n`;
    !transStream.closed && transStream.write(data);
  };
  // 写入有效输出，首条输出前补上role chunk
  const writeOutput = (delta: any) => {
    if (!producedOutput) {
      producedOutput = true;
      writeChunk({ role: 'assistant', content: '' });
    }
    writeChunk(delta);
  };
  // 收尾
  const finalize = () => {
    if (finished)
      return;
    finished = true;
    if (!producedOutput) {
      producedOutput = true;
      writeChunk({ role: 'assistant', content: '' });
    }
    writeChunk({}, 'stop', { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 });
    !transStream.closed && transStream.end('data: [DONE]\n\n');
    endCallback && endCallback();
  };
  // 上游未产生输出即失败时（如免费账号被限流），退避后换一条上游流重试
  const tryRetry = (reason: string) => {
    if (retrying || !retryCallback || retryCount >= MAX_RETRY_COUNT)
      return false;
    retrying = true;
    const delay = RETRY_DELAY * Math.pow(2, retryCount);
    logger.warn(`上游未产生输出(${reason})，${delay / 1000}s 后自动重试...`);
    // 断开当前上游流，避免继续消费
    stream.destroy && stream.destroy();
    setTimeout(async () => {
      try {
        const nextStream = await retryCallback();
        retrying = false;
        if (transStream.closed)
          return;
        nextStream.on("data", (chunk: Buffer) => !transStream.closed && transStream.write(chunk));
        nextStream.once("error", (err: any) => {
          logger.error(err);
          finalize();
        });
        nextStream.once("end", () => !transStream.closed && transStream.end());
      }
      catch (err: any) {
        logger.error(err);
        retrying = false;
        writeOutput({ content: err.message });
        finalize();
      }
    }, delay);
    return true;
  };
  const feed = createConnectStreamParser(
    event => {
      updateContextFromEvent(context, event);
      const delta = extractDelta(event, currentPhase);
      currentPhase = delta.phase || null;
      // 思考过程
      if (delta.reasoning)
        writeOutput({ reasoning_content: trimInvalidChar(delta.reasoning) });
      // 正式回答
      if (delta.content)
        writeOutput({ content: trimInvalidChar(delta.content) });
      // 上游可能先发送空done再补发错误帧，因此这里不立即结束，统一等流关闭后收尾
    },
    err => {
      logger.error(err);
      // 尚未输出任何内容时优先换流重试，否则把失败原因告知客户端
      if (!producedOutput && tryRetry(err.message))
        return;
      if (!producedOutput)
        writeOutput({ content: err.message });
      finalize();
    }
  );
  // 将流数据喂给Connect协议解析器
  stream.on("data", (chunk: Buffer) => {
    if (retrying || finished)
      return;
    try {
      feed(chunk);
    }
    catch (err) {
      logger.error(err);
      finalize();
    }
  });
  stream.once("error", (err: any) => {
    if (!producedOutput && tryRetry(err.message))
      return;
    finalize();
  });
  stream.once("close", () => {
    // 换流重试中，交由重试流程接管
    if (retrying)
      return;
    // 未产生任何输出即结束，视为上游失败
    if (!producedOutput && tryRetry('上游未返回内容'))
      return;
    finalize();
  });
  return transStream;
}

/**
 * Token切分
 * 
 * @param authorization 认证字符串
 */
function tokenSplit(authorization: string) {
  return authorization.replace('Bearer ', '').split(',');
}

/**
 * 获取Token存活状态
 */
async function getTokenLiveStatus(refreshToken: string) {
  const result = await axios.get('https://kimi.moonshot.cn/api/auth/token/refresh', {
    headers: {
      Authorization: `Bearer ${refreshToken}`,
      Referer: 'https://kimi.moonshot.cn/',
      ...FAKE_HEADERS
    },
    timeout: 15000,
    validateStatus: () => true
  });
  try {
    const {
      access_token,
      refresh_token
    } = checkResult(result, refreshToken);
    return !!(access_token && refresh_token)
  }
  catch (err) {
    return false;
  }
}

export default {
  createCompletion,
  createCompletionStream,
  getTokenLiveStatus,
  tokenSplit
};
