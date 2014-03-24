package cn.box.lua;

import android.app.Activity;
import cn.box.play.widget.PlaySettingController;
import cn.box.utils.CommonUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eric on 14-3-24.
 *
 * 根据不同的策略选择 使用服务端地址、前端抓取地址、重抓地址，并可根据播放错误在三种地址间做切换
 */
public class UrlController {

    private static final String TAG = "UrlController";

    // Status: Init
    private static final int STATUS_INIT = 0x00;
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

    // Event: Start
    private static final int EVENT_START = 0x00;
    // Event: Try Url, Success Response from MediaPlayer
    private static final int EVENT_PLAY_SUCCESS = 0x01;
    // Event: Try Url, Failed Response from MediaPlayer
    private static final int EVENT_PLAY_FAILED = 0x02;
    // Event: Lua Crawl Failed or Timeout
    private static final int EVENT_LUA_FAILED_TIMEOUT= 0x03;
    // Event: Server Retry Response Failed
    private static final int EVENT_RETRY_REQUEST_FAILED = 0x04;

    // Action When enter status
    // Action: Run lua script when enter status: STATUS_USING_LUA_URL
    private static final int ACTION_START_LUA_CRAWL = 0x01;
    // Action: Run lua script when enter status: STATUS_USING_RETRY_URL
    private static final int ACTION_RETRY_GET_URL = 0x02;
    // Action: Try to change source when all three kinds url failed
    private static final int ACTION_TRY_CHANGE_SOURCE = 0x03;
    // Action: Notify Url is available
    private static final int ACTION_URL_AVAILABLE = 0x04;
    // Action: Do Nothing
    private static final int ACTION_DO_NOTHING = 0x05;

    /**
     * 播放地址确定的回调
     */
    public interface UrlDetermineListener {
        public void onPlayUrlDetermined(String playUrl);
    }

    /**
     * 根据Lua控制信息获取Url策略
     * @param luaInfo
     * @return
     */
//    public static UrlStrategy getUrlStrategy(LuaControlInfo luaInfo) {
//        return new DefaultStrategy();
//    }

    /**
     * 策略类
     */
    static abstract class UrlStrategy {

        private UrlDetermineListener mUrlDetermineListener;
        private PlaySettingController mSettingController;
        private Activity mActivity;
        private LuaControlInfo mLuaInfo;

        protected List<Transition> transitions = new ArrayList<Transition>();
        protected int mCurState = STATUS_INIT;

        protected UrlStrategy(UrlDetermineListener urlDetermineListener, PlaySettingController settingController) {
            this.mUrlDetermineListener = urlDetermineListener;
            this.mSettingController = settingController;
            enterState(getInitTransition());
        }

        protected void enterState(Transition transition) {
            if (transition != null) {
                mCurState = transition.action();
            }
        }

        protected int getState() {
            return mCurState;
        }

        protected void handle(int event) {
            int curState = getState();
            List<Transition> transitions = getTransitionList();

            if (CommonUtils.isListEmpty(transitions)) {
                return;
            }

            for (Transition transition : transitions) {
                if (curState == transition.startState && event == transition.event) {
                    enterState(transition);
                    break;
                }
            }
        }

        public void doAction(int action) {
            switch(action) {
                case ACTION_RETRY_GET_URL:
                    break;
                case ACTION_URL_AVAILABLE:
                    break;
                case ACTION_START_LUA_CRAWL:
                    break;
                case ACTION_TRY_CHANGE_SOURCE:
                    break;
                case ACTION_DO_NOTHING:
                    break;
            }
        }

        public String getUrl() {
            return mSettingController.getUrl();
        }

        public void onPlaySuccess() {
            handle(EVENT_PLAY_SUCCESS);
        }

        public void onPlayFailed() {
            handle(EVENT_PLAY_FAILED);
        }

        public void onRetryRequestFailed() {
            handle(EVENT_RETRY_REQUEST_FAILED);
        }

        public void onLuaFailedTimeout() {
            handle(EVENT_LUA_FAILED_TIMEOUT);
        }

        protected abstract Transition getInitTransition();

        protected abstract void setTransitionList(List<Transition> transitionList);
    }

    /**
     * 默认策略，服务端地址-> 重抓 -> 切源/失败
     */
    static class DefaultStrategy extends UrlStrategy {

        public DefaultStrategy(UrlDetermineListener urlDetermineListener, PlaySettingController settingController) {
            super(urlDetermineListener, settingController);
        }

        @Override
        protected Transition getInitTransition() {
            return new Transition(this, STATUS_INIT, EVENT_START, STATUS_USING_SERVER_URL, ACTION_URL_AVAILABLE);
        }

        @Override
        protected void setTransitionList(List<Transition> transitionList) {
            transitions.add(new Transition(this, STATUS_USING_SERVER_URL, EVENT_PLAY_SUCCESS, STATUS_URL_SUCCESS, ACTION_DO_NOTHING));
            transitions.add(new Transition(this, STATUS_USING_SERVER_URL, EVENT_PLAY_FAILED, STATUS_USING_RETRY_URL, ACTION_RETRY_GET_URL));
            transitions.add(new Transition(this, STATUS_USING_RETRY_URL, EVENT_PLAY_SUCCESS, STATUS_URL_SUCCESS, ACTION_DO_NOTHING));
            transitions.add(new Transition(this, STATUS_USING_RETRY_URL, EVENT_PLAY_FAILED, STATUS_URL_FAILED, ACTION_TRY_CHANGE_SOURCE));
        }

    }

    /**
     * 前端优先策略， 前端抓取地址-> 服务端地址 -> 重抓 -> 切源/失败
     */
    static class LuaPriorStrategy extends UrlStrategy {

        protected LuaPriorStrategy(UrlDetermineListener urlDetermineListener, PlaySettingController settingController) {
            super(urlDetermineListener, settingController);
        }

        @Override
        protected Transition getInitTransition() {
            return new Transition(this, STATUS_INIT, EVENT_START, STATUS_USING_LUA_URL, ACTION_START_LUA_CRAWL);
        }

        @Override
        protected void setTransitionList(List<Transition> transitionList) {
            transitionList.add(new Transition(this, ))
        }
    }

    /**
     * 后端优先策略， 服务端地址-> 前端抓取地址 -> 重抓 -> 切源/失败
     */
    static class ServerPriorStrategy extends UrlStrategy {

        protected ServerPriorStrategy(UrlDetermineListener urlDetermineListener, PlaySettingController settingController) {
            super(urlDetermineListener, settingController);
        }

        @Override
        protected Transition getInitTransition() {
            return new Transition(this, STATUS_INIT, EVENT_START, STATUS_USING_RETRY_URL, ACTION_RETRY_GET_URL);
        }

        @Override
        protected void setTransitionList(List<Transition> transitionList) {

        }
    }

    /**
     * 状态迁移类
     */
    static class Transition {

        UrlStrategy urlStrategy;

        int startState;
        int event;
        int endState;
        int action;

        Transition(UrlStrategy urlStrategy, int startState, int event, int endState, int action) {
            this.urlStrategy = urlStrategy;
            this.startState = startState;
            this.event = event;
            this.endState = endState;
            this.action = action;
        }

        public int action() {
            urlStrategy.doAction(action);
            return endState;
        }
    }

}
