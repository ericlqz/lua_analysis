package cn.box.lua;

import cn.box.play.utils.Log;
import cn.box.utils.CommonUtils;
import net.tsz.afinal.core.Arrays;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by eric on 14-3-24.
 *
 * 根据不同的策略选择 使用服务端地址、前端抓取地址、重抓地址，并可根据播放错误在三种地址间做切换
 */
public class UrlController {

    private static final String TAG = "UrlController";

    // Status: Using Url from Server
    private static final int STATUS_USING_SERVER_URL = 0x01;
    // Status: Using Url from Lua Script
    private static final int STATUS_USING_LUA_URL = 0x02;
    // Status: Using Url from Server Retry
    private static final int STATUS_USING_RETRY_URL = 0x03;
    // Status: Final Play Success
    private static final int STATUS_URL_SUCCESS = 0x04;
    // Status: Final Play Failed
    private static final int STATUS_URL_FAILED = 0x05;

    // Event: Try Url, Success Response from MediaPlayer
    public static final int EVENT_PLAY_SUCCESS = 0x01;
    // Event: Try Url, Failed Response from MediaPlayer
    public static final int EVENT_PLAY_FAILED = 0x02;
    // Event: Lua Crawl Failed or Timeout
    public static final int EVENT_LUA_FAILED_TIMEOUT= 0x03;
    // Event: Server Retry Response Failed
    public static final int EVENT_RETRY_REQUEST_FAILED = 0x04;

    // Action When enter status
    // Action: Run lua script when enter status: STATUS_USING_LUA_URL
    private static final int ACTION_START_LUA_CRAWL = 0x01;
    // Action: Run lua script when enter status: STATUS_USING_RETRY_URL
    private static final int ACTION_RETRY_GET_URL = 0x02;
    // Action: Try to change source when all three kinds url failed
    private static final int ACTION_NOTIFY_ERROR = 0x03;
    // Action: Notify Url is available
    private static final int ACTION_URL_AVAILABLE = 0x04;

    private static final int RESPONSE_DONE = 0x01;
    private static final int RESPONSE_CONTINUE = 0x02;

    /**
     * 播放地址确定的回调
     */
    public interface UrlDetermineListener {
        public void onPlayUrlDetermined(String playUrl);
    }

    private HashMap<Integer, List<Integer>> statusToEvent = new HashMap<Integer, List<Integer>>();
    private HashMap<Integer, Integer> eventToResponse = new HashMap<Integer, Integer>();
    private HashMap<Integer, Integer> statusToAction = new HashMap<Integer, Integer>();

    private String mPlayUrl;
    private int mCurStatus = -1;
    private List<Transition> mTransitionTable = new ArrayList<Transition>();
    private UrlDetermineListener mUrlDetermineListener;

    public UrlController() {

        // 初始化 状态 - 事件 的映射
        statusToEvent.put(STATUS_USING_SERVER_URL, Arrays.asList(EVENT_PLAY_FAILED, EVENT_PLAY_SUCCESS));
        statusToEvent.put(STATUS_USING_LUA_URL, Arrays.asList(EVENT_PLAY_FAILED, EVENT_PLAY_SUCCESS, EVENT_LUA_FAILED_TIMEOUT));
        statusToEvent.put(STATUS_USING_RETRY_URL, Arrays.asList(EVENT_PLAY_FAILED, EVENT_PLAY_SUCCESS, EVENT_RETRY_REQUEST_FAILED));
        statusToEvent.put(STATUS_URL_SUCCESS, Arrays.asList(EVENT_PLAY_FAILED));

        // 初始化 事件 - 响应 的映射
        eventToResponse.put(EVENT_PLAY_FAILED, RESPONSE_CONTINUE);
        eventToResponse.put(EVENT_PLAY_SUCCESS, RESPONSE_DONE);
        eventToResponse.put(EVENT_LUA_FAILED_TIMEOUT, RESPONSE_CONTINUE);
        eventToResponse.put(EVENT_RETRY_REQUEST_FAILED, RESPONSE_CONTINUE);

        // 初始化 状态 - Action 的映射
        statusToAction.put(STATUS_USING_SERVER_URL, ACTION_URL_AVAILABLE);
        statusToAction.put(STATUS_USING_LUA_URL, ACTION_START_LUA_CRAWL);
        statusToAction.put(STATUS_USING_RETRY_URL, ACTION_RETRY_GET_URL);
        statusToAction.put(STATUS_URL_FAILED, ACTION_NOTIFY_ERROR);
    }

    /**
     * 根据传入的策略生成状态迁移表
     * 当前存在三种播放地址：服务端返回地址(0x01)、Lua前端抓取返回地址(0x02)、请求服务端即时重抓地址(0x03)
     * 每种策略都是 三种地址中至少两种地址 的排序
     * 如
     *  默认策略: [1, 3]服务端返回地址、请求服务端即时重抓地址，表示 若服务端返回地址播放失败，则尝试服务端即时重抓，若仍然失败，则抛出错误。
     *  其它可选策略: [2, 1, 3] 表示按照 Lua前端抓取返回地址、服务端返回地址、请求服务端即时重抓地址 的顺序
     * @param strategy
     */
    public void applyStrategy(int[] strategy) {
        if (strategy == null || strategy.length <= 1) {
            Log.d(TAG, "Strategy Empty. Set to default strategy.");
            strategy = getDefaultStrategy();
        }

        // 生成状态迁移表
        for (int i = 0; i < strategy.length; i++) {
            int startStatus = strategy[i];
            List<Integer> availableEvents = statusToEvent.get(startStatus);
            if (CommonUtils.isListNotEmpty(availableEvents)) {
                for (Integer event : availableEvents) {
                    Integer response = eventToResponse.get(event);
                    if (response != null) {

                        int nextStatus;
                        if (i + 1 < strategy.length) {
                            nextStatus = strategy[i+1];
                        } else {
                            nextStatus = STATUS_URL_FAILED;
                        }

                        int endStatus = (response == RESPONSE_DONE) ? STATUS_URL_SUCCESS : nextStatus;
                        Log.d(TAG, "generate transition [" + startStatus +  ", " + event + ", " + endStatus + "]");
                        mTransitionTable.add(new Transition(startStatus, event, endStatus));
                    }
                }
            }
        }

        // 进入初始状态
        Log.d(TAG, "enter initial status: " + strategy[0]);
        enterStatus(strategy[0]);
    }

    /**
     * 设置播放默认策略 [1, 3]
     */
    private int[] getDefaultStrategy() {
        return new int[] {STATUS_USING_SERVER_URL, STATUS_USING_RETRY_URL};
    }

    /**
     * 响应事件处理，切换状态
     * @param event
     */
    public void handle(int event) {
        for (Transition transition : mTransitionTable) {
            if (mCurStatus == transition.startStatus && transition.event == event) {
                enterStatus(transition.endStatus);
                break;
            }
        }
    }

    /**
     * 根据当前状态返回播放链接
     * @return
     */
    public String getUrl() {
        return null;
    }

    /**
     * 设置Lua及重抓地址成功时的回调
     * @param urlDetermineListener
     */
    public void setUrlDetermineListener(UrlDetermineListener urlDetermineListener) {
        this.mUrlDetermineListener = urlDetermineListener;
    }

    /**
     * 进入某状态，并执行相应的Action
     * @param status
     */
    private void enterStatus(int status) {
        Integer action = statusToAction.get(status);
        if (action != null) {
            doAction(action);
        }

        mCurStatus = status;

        // TODO: 日志
    }

    /**
     * 执行某Action
     * @param action
     */
    private void doAction(int action) {
        switch(action) {
            case ACTION_START_LUA_CRAWL:
                break;
            case ACTION_URL_AVAILABLE:
                break;
            case ACTION_RETRY_GET_URL:
                break;
            case ACTION_NOTIFY_ERROR:
                break;
        }
    }

    /**
     * 状态迁移表项
     */
    class Transition {
        int startStatus;
        int event;
        int endStatus;

        Transition(int startStatus, int event, int endStatus) {
            this.startStatus = startStatus;
            this.event = event;
            this.endStatus = endStatus;
        }
    }

}
