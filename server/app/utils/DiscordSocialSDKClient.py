import asyncio
import ctypes
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar

from app import logging
from app.constants import DISCORD_CLIENT_ID, DISCORD_SOCIAL_SDK_PATH


class _Handle(ctypes.Structure):
    """ Discord Social SDK の C API で使われる不透明なハンドル (Discord_Client や Discord_Activity など) """
    _fields_ = [('opaque', ctypes.c_void_p)]


class _String(ctypes.Structure):
    """ Discord Social SDK の C API で使われる UTF-8 文字列 (Discord_String) """
    _fields_ = [('ptr', ctypes.POINTER(ctypes.c_uint8)), ('size', ctypes.c_size_t)]


# Discord Social SDK の C API (cdiscord.h) のうち、このクライアントで使う関数の戻り値の型と引数の型
_HANDLE_P = ctypes.POINTER(_Handle)
_STRING_P = ctypes.POINTER(_String)
## 結果を受け取るコールバック (Discord_Client_UpdateTokenCallback / Discord_Client_UpdateRichPresenceCallback)
_RESULT_CALLBACK = ctypes.CFUNCTYPE(None, _HANDLE_P, ctypes.c_void_p)
_FUNCTIONS: dict[str, tuple[Any, list[Any]]] = {
    'Discord_RunCallbacks': (None, []),
    'Discord_Free': (None, [ctypes.c_void_p]),
    'Discord_Client_Init': (None, [_HANDLE_P]),
    'Discord_Client_Drop': (None, [_HANDLE_P]),
    'Discord_Client_SetApplicationId': (None, [_HANDLE_P, ctypes.c_uint64]),
    'Discord_Client_UpdateToken': (None, [_HANDLE_P, ctypes.c_int, _String, _RESULT_CALLBACK, ctypes.c_void_p, ctypes.c_void_p]),
    'Discord_Client_Connect': (None, [_HANDLE_P]),
    'Discord_Client_GetStatus': (ctypes.c_int, [_HANDLE_P]),
    'Discord_Client_UpdateRichPresence': (None, [_HANDLE_P, _HANDLE_P, _RESULT_CALLBACK, ctypes.c_void_p, ctypes.c_void_p]),
    'Discord_Client_ClearRichPresence': (None, [_HANDLE_P]),
    'Discord_ClientResult_Successful': (ctypes.c_bool, [_HANDLE_P]),
    'Discord_ClientResult_Error': (None, [_HANDLE_P, _STRING_P]),
    'Discord_Activity_Init': (None, [_HANDLE_P]),
    'Discord_Activity_Drop': (None, [_HANDLE_P]),
    'Discord_Activity_SetType': (None, [_HANDLE_P, ctypes.c_int]),
    'Discord_Activity_SetState': (None, [_HANDLE_P, _STRING_P]),
    'Discord_Activity_SetDetails': (None, [_HANDLE_P, _STRING_P]),
    'Discord_Activity_SetAssets': (None, [_HANDLE_P, _HANDLE_P]),
    'Discord_Activity_SetTimestamps': (None, [_HANDLE_P, _HANDLE_P]),
    'Discord_ActivityAssets_Init': (None, [_HANDLE_P]),
    'Discord_ActivityAssets_Drop': (None, [_HANDLE_P]),
    'Discord_ActivityAssets_SetLargeImage': (None, [_HANDLE_P, _STRING_P]),
    'Discord_ActivityAssets_SetLargeText': (None, [_HANDLE_P, _STRING_P]),
    'Discord_ActivityAssets_SetSmallImage': (None, [_HANDLE_P, _STRING_P]),
    'Discord_ActivityAssets_SetSmallText': (None, [_HANDLE_P, _STRING_P]),
    'Discord_ActivityTimestamps_Init': (None, [_HANDLE_P]),
    'Discord_ActivityTimestamps_Drop': (None, [_HANDLE_P]),
    'Discord_ActivityTimestamps_SetStart': (None, [_HANDLE_P, ctypes.c_uint64]),
    'Discord_ActivityTimestamps_SetEnd': (None, [_HANDLE_P, ctypes.c_uint64]),
}

# Discord_AuthorizationTokenType_Bearer (OAuth で取得したアクセストークン)
_TOKEN_TYPE_BEARER = 1
# Discord_Client_Status_Ready (Discord に接続済みで、Rich Presence を設定できる状態)
_STATUS_READY = 3


def _ToString(value: str) -> _String:
    """
    Python の文字列を Discord_String に変換する
    Discord_String はバッファを参照するだけなので、戻り値を使い終わるまでは参照を保持しておく必要がある

    Args:
        value (str): 変換する文字列

    Returns:
        _String: 変換後の Discord_String
    """

    data = value.encode('utf-8')
    buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data)
    string = _String(ctypes.cast(buffer, ctypes.POINTER(ctypes.c_uint8)), len(data))
    # バッファが先に解放されないよう、Discord_String に参照を持たせておく
    setattr(string, '_buffer', buffer)
    return string


class DiscordSocialSDKClient:
    """
    Discord Social SDK を使い、OAuth で連携した Discord アカウントに Rich Presence (アクティビティ) を設定するクライアント
    Discord クライアントとの IPC と異なり、KonomiTV サーバーと同じ PC で Discord が起動していなくても表示できる
    連携したアカウント (KonomiTV のユーザー) ごとに1つのインスタンスを使う
    """

    # 読み込んだ Discord Social SDK の共有ライブラリ (未読み込みの場合は None)
    ## 全インスタンスで共有し、_loadLibrary() で一度だけ読み込む
    _library: ClassVar[ctypes.CDLL | None] = None

    # Discord Social SDK の関数を呼び出す専用のスレッド
    ## SDK はスレッドセーフではなく、コールバックも Discord_RunCallbacks() を呼んだスレッドで実行されるため、すべての呼び出しを1つのスレッドに集約する
    _executor: ClassVar[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=1, thread_name_prefix='DiscordSocialSDK')

    # 接続中のインスタンスの数と、その間 Discord_RunCallbacks() を定期的に呼び出す asyncio タスク
    ## 接続中のインスタンスがなくなると _pumpCallbacks() は終了し、タスクは None に戻る
    _client_count: ClassVar[int] = 0
    _pump_task: ClassVar[asyncio.Task[None] | None] = None


    def __init__(self) -> None:
        """
        DiscordSocialSDKClient を初期化する
        """

        # Discord Social SDK のクライアント (Discord_Client)
        ## connect() で初期化され、close() で破棄される (未接続・破棄済みの場合は None)
        self._client: _Handle | None = None

        # 結果を受け取るコールバック
        ## C 側から呼ばれる間に解放されないよう、インスタンスで参照を保持しておく
        self._update_token_callback = _RESULT_CALLBACK(self._onTokenUpdated)
        self._update_rich_presence_callback = _RESULT_CALLBACK(self._onRichPresenceUpdated)

        # 最後に Discord に設定したアクティビティ
        ## setActivity() で前回と同じ内容を何度も送信しないために参照する
        ## 接続が切れたときは isReady() で、設定に失敗したときは _onRichPresenceUpdated() で None に戻し、次回送り直させる
        self._last_activity: dict[str, Any] | None = None


    @classmethod
    def _loadLibrary(cls) -> ctypes.CDLL | None:
        """
        Discord Social SDK の共有ライブラリを読み込み、使う関数の型を設定する

        Returns:
            ctypes.CDLL | None: 読み込んだライブラリ (読み込めなかった場合は None)
        """

        if cls._library is None:
            try:
                library = ctypes.CDLL(str(DISCORD_SOCIAL_SDK_PATH))
                for name, (restype, argtypes) in _FUNCTIONS.items():
                    function = getattr(library, name)
                    function.restype = restype
                    function.argtypes = argtypes
                cls._library = library
            except (OSError, AttributeError) as ex:
                logging.error(f'[DiscordSocialSDKClient] Failed to load Discord Social SDK. ({DISCORD_SOCIAL_SDK_PATH})', exc_info=ex)
        return cls._library


    @classmethod
    def isAvailable(cls) -> bool:
        """
        Discord Social SDK を読み込めて、アカウント連携による Rich Presence を使えるかどうか

        Returns:
            bool: 使える場合は True
        """

        return cls._loadLibrary() is not None


    @classmethod
    async def _call(cls, function: Callable[..., Any], *args: Any) -> Any:
        """
        Discord Social SDK の関数を専用のスレッドで実行する

        Args:
            function (Callable[..., Any]): 実行する関数
            *args (Any): 関数に渡す引数

        Returns:
            Any: 関数の戻り値
        """

        return await asyncio.get_running_loop().run_in_executor(cls._executor, function, *args)


    @classmethod
    async def _pumpCallbacks(cls) -> None:
        """
        接続中のインスタンスがある間、Discord_RunCallbacks() を定期的に呼び出して SDK のコールバックを実行させる
        """

        while cls._client_count > 0 and cls._library is not None:
            await cls._call(cls._library.Discord_RunCallbacks)
            await asyncio.sleep(0.1)
        cls._pump_task = None


    async def connect(self, access_token: str) -> bool:
        """
        OAuth で取得したアクセストークンで Discord に接続する
        接続は非同期に行われるため、Rich Presence を設定する前に isReady() で接続済みか確認する必要がある

        Args:
            access_token (str): Discord OAuth のアクセストークン (openid / sdk.social_layer_presence スコープが必要)

        Returns:
            bool: 接続を開始できた場合は True (Discord Social SDK を読み込めなかった場合は False)
        """

        library = self._loadLibrary()
        if library is None:
            return False

        # クライアントを初期化し、アクセストークンを設定する
        ## アクセストークンの設定が完了すると _onTokenUpdated() が呼ばれ、そこで接続を開始する
        def Connect() -> None:
            self._client = _Handle()
            library.Discord_Client_Init(self._client)
            library.Discord_Client_SetApplicationId(self._client, int(DISCORD_CLIENT_ID))
            library.Discord_Client_UpdateToken(
                self._client, _TOKEN_TYPE_BEARER, _ToString(access_token), self._update_token_callback, None, None,
            )
        await self._call(Connect)

        # コールバックを実行させるタスクを (未起動なら) 起動する
        DiscordSocialSDKClient._client_count += 1
        if DiscordSocialSDKClient._pump_task is None:
            DiscordSocialSDKClient._pump_task = asyncio.create_task(self._pumpCallbacks())
        return True


    async def isReady(self) -> bool:
        """
        Discord に接続済みで、Rich Presence を設定できる状態かどうか

        Returns:
            bool: Rich Presence を設定できる状態なら True
        """

        library = self._library
        if library is None or self._client is None:
            return False
        is_ready = await self._call(lambda: library.Discord_Client_GetStatus(self._client) == _STATUS_READY)
        # 再接続後は Discord 側のアクティビティが消えているため、前回の送信内容をリセットして必ず送り直させる
        if is_ready is False:
            self._last_activity = None
        return is_ready


    async def setActivity(self, activity: dict[str, Any] | None) -> None:
        """
        Rich Presence (アクティビティ) を設定する
        アクティビティは DiscordRichPresenceTask._getActivity() で生成した、Discord の IPC と同じ形式の dict を受け取る
        設定の結果は非同期に _onRichPresenceUpdated() で受け取る

        Args:
            activity (dict[str, Any] | None): 設定するアクティビティ (None を渡すとアクティビティを消去する)
        """

        # 前回送信した内容から変化があったときだけ送信する
        library = self._library
        if library is None or self._client is None or activity == self._last_activity:
            return
        self._last_activity = activity

        def SetActivity() -> None:
            # アクティビティを消去する
            if activity is None:
                library.Discord_Client_ClearRichPresence(self._client)
                return

            # アクティビティの種類と、詳細・状態のテキストを設定する
            ## 各 Set 関数は値をコピーするため、設定後すぐに Drop してよい
            activity_handle, assets_handle, timestamps_handle = _Handle(), _Handle(), _Handle()
            library.Discord_Activity_Init(activity_handle)
            library.Discord_Activity_SetType(activity_handle, activity['type'])
            library.Discord_Activity_SetDetails(activity_handle, ctypes.byref(_ToString(activity['details'])))
            library.Discord_Activity_SetState(activity_handle, ctypes.byref(_ToString(activity['state'])))

            # 大きい画像・小さい画像とそのテキストを設定する
            library.Discord_ActivityAssets_Init(assets_handle)
            for key, setter in (
                ('large_image', library.Discord_ActivityAssets_SetLargeImage),
                ('large_text', library.Discord_ActivityAssets_SetLargeText),
                ('small_image', library.Discord_ActivityAssets_SetSmallImage),
                ('small_text', library.Discord_ActivityAssets_SetSmallText),
            ):
                if key in activity['assets']:
                    setter(assets_handle, ctypes.byref(_ToString(activity['assets'][key])))
            library.Discord_Activity_SetAssets(activity_handle, assets_handle)

            # 番組の開始・終了時刻 (UNIX ミリ秒) があれば設定する
            library.Discord_ActivityTimestamps_Init(timestamps_handle)
            if 'timestamps' in activity:
                library.Discord_ActivityTimestamps_SetStart(timestamps_handle, activity['timestamps']['start'])
                library.Discord_ActivityTimestamps_SetEnd(timestamps_handle, activity['timestamps']['end'])
                library.Discord_Activity_SetTimestamps(activity_handle, timestamps_handle)

            library.Discord_Client_UpdateRichPresence(self._client, activity_handle, self._update_rich_presence_callback, None, None)
            library.Discord_ActivityTimestamps_Drop(timestamps_handle)
            library.Discord_ActivityAssets_Drop(assets_handle)
            library.Discord_Activity_Drop(activity_handle)

        await self._call(SetActivity)


    async def close(self) -> None:
        """
        Discord との接続を閉じてクライアントを破棄する
        接続が閉じられると、Discord 側でこのクライアントが設定したアクティビティは自動的に消去される
        """

        library = self._library
        if library is None or self._client is None:
            return
        client, self._client = self._client, None
        await self._call(library.Discord_Client_Drop, client)
        DiscordSocialSDKClient._client_count -= 1


    def _getResultError(self, result: Any) -> str | None:
        """
        Discord_ClientResult から、失敗した場合のエラーメッセージを取得する (SDK のスレッドで実行される)

        Args:
            result (Any): Discord_ClientResult へのポインタ

        Returns:
            str | None: 失敗した場合はエラーメッセージ、成功した場合は None
        """

        assert self._library is not None
        if self._library.Discord_ClientResult_Successful(result) is True:
            return None
        error = _String()
        self._library.Discord_ClientResult_Error(result, ctypes.byref(error))
        message = ctypes.string_at(error.ptr, error.size).decode('utf-8', errors='replace') if error.ptr else 'Unknown error'
        # SDK から返された文字列は SDK が確保したメモリなので、SDK に解放させる
        self._library.Discord_Free(ctypes.cast(error.ptr, ctypes.c_void_p))
        return message


    def _onTokenUpdated(self, result: Any, user_data: Any) -> None:
        """
        アクセストークンの設定が完了したときに呼ばれるコールバック (SDK のスレッドで実行される)
        設定に成功していれば、Discord への接続を開始する
        """

        error = self._getResultError(result)
        if error is not None:
            logging.warning(f'[DiscordSocialSDKClient] Failed to update token: {error}')
            return
        if self._library is not None and self._client is not None:
            self._library.Discord_Client_Connect(self._client)
            logging.info('[DiscordSocialSDKClient] Connecting to Discord with linked account.')


    def _onRichPresenceUpdated(self, result: Any, user_data: Any) -> None:
        """
        Rich Presence の設定が完了したときに呼ばれるコールバック (SDK のスレッドで実行される)
        設定に失敗した場合は、次回送り直すために _last_activity をリセットする
        """

        error = self._getResultError(result)
        if error is not None:
            logging.warning(f'[DiscordSocialSDKClient] Failed to update rich presence: {error}')
            self._last_activity = None
