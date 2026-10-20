
import asyncio
import contextlib
from typing import Any
from urllib.parse import quote

from app import logging
from app.config import ClientSettings
from app.constants import DISCORD_CLIENT_ID, LOGO_DIR
from app.models.Channel import Channel
from app.models.User import User
from app.streams.LiveStream import LiveStream
from app.utils.DiscordRPCClient import DiscordRPCClient
from app.utils.DiscordSocialSDKClient import DiscordSocialSDKClient


class DiscordRichPresenceTask:
    """
    視聴中のテレビ番組を、Discord の Rich Presence に定期的に反映するタスク
    KonomiTV サーバーと同じ PC で起動している Discord への IPC と、Discord Social SDK を使った連携アカウントへの表示の両方に対応する
    """

    # アクティビティを更新する間隔 (秒)
    UPDATE_INTERVAL = 15

    # 局ロゴの公開 URL のベース
    ## Discord は外部 URL の画像をアクティビティに表示できるが、KonomiTV サーバーはインターネットから到達できないとは限らないため、
    ## GitHub 上にある同梱ロゴ (server/static/logos/) の URL を使う
    LOGO_BASE_URL = 'https://raw.githubusercontent.com/tsukumijima/KonomiTV/master/server/static/logos/'

    # KonomiTV のアイコンの公開 URL
    KONOMITV_ICON_URL = 'https://raw.githubusercontent.com/tsukumijima/KonomiTV/master/client/public/assets/images/icons/icon-192px.png'


    def __init__(self) -> None:
        """
        DiscordRichPresenceTask を初期化する
        """

        # Discord クライアントとの IPC 接続を管理するクライアント
        ## _update() で接続・アクティビティ設定を行い、stop() で接続を閉じる
        self._client = DiscordRPCClient(DISCORD_CLIENT_ID)

        # アクティビティを定期的に更新する asyncio タスク
        ## start() で生成され、stop() でキャンセルされる (未開始・停止済みの場合は None)
        self._task: asyncio.Task[None] | None = None

        # 最後に Discord に設定したアクティビティ
        ## _update() で前回と同じ内容のアクティビティを何度も送信しないために参照する
        ## 未設定の場合や再接続した直後は None になる
        self._last_activity: dict[str, Any] | None = None

        # アカウント連携で表示する場合の、KonomiTV のユーザー ID ごとの Discord Social SDK のクライアント
        ## _updateSocialSDK() で視聴開始時に接続し、表示対象でなくなったときや stop() で接続を閉じる
        self._social_sdk_clients: dict[int, DiscordSocialSDKClient] = {}


    async def start(self) -> None:
        """
        アクティビティの定期更新を開始する
        """

        if self._task is None:
            self._task = asyncio.create_task(self._run())


    async def stop(self) -> None:
        """
        アクティビティの定期更新を停止し、Discord クライアントとの接続を閉じる
        """

        # 定期更新タスクをキャンセルし、終了を待つ
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

        # 接続を閉じると、Discord 側でアクティビティが消去される
        await self._client.close()
        for client in self._social_sdk_clients.values():
            await client.close()
        self._social_sdk_clients.clear()


    async def _run(self) -> None:
        """
        UPDATE_INTERVAL 秒ごとにアクティビティを更新し続ける
        """

        while True:
            # 更新中に予期しないエラーが発生しても定期更新自体は止めない
            try:
                await self._update()
            except Exception as ex:
                logging.error('[DiscordRichPresenceTask] Failed to update activity:', exc_info=ex)
            await asyncio.sleep(self.UPDATE_INTERVAL)


    async def _update(self) -> None:
        """
        現在の視聴状況に応じて、Discord のアクティビティを1回更新する
        """

        # 視聴中のクライアントがいるライブストリームのうち、最初の1つを対象にする
        ## KonomiTV サーバーではどのユーザーが視聴しているかは区別できないため、複数ある場合は1つだけ表示する
        ## メモリ上の情報だけで判定できるため、DB へのアクセスや Discord への接続より先に行う
        live_stream = next((
            live_stream for live_stream in LiveStream.getAllLiveStreams()
            if live_stream.getStatus().client_count > 0
        ), None)

        # サーバーに同期されたクライアント設定で discord_rich_presence がオンのユーザーを、表示方法ごとに振り分ける
        ## IPC: いずれかのユーザーでオンなら、同じ PC で起動している Discord に1つだけ表示する
        ## AccountLinking: Discord アカウントと連携済みのユーザーごとに、そのアカウントに表示する
        ## 誰も視聴していない場合は設定に関わらず表示するものがないため、DB へのアクセスを省略する
        is_ipc_enabled = False
        linked_users: list[User] = []
        for user in (await User.all() if live_stream is not None else []):
            client_settings = ClientSettings.model_validate(user.client_settings)
            if client_settings.discord_rich_presence is False:
                continue
            if client_settings.discord_rich_presence_connection_type == 'AccountLinking':
                if user.discord_refresh_token is not None:
                    linked_users.append(user)
            else:
                is_ipc_enabled = True

        # 表示するアクティビティを生成する
        activity = None
        if live_stream is not None and (is_ipc_enabled is True or len(linked_users) > 0):
            activity = await self._getActivity(live_stream)

        await self._updateIPC(is_ipc_enabled, activity)
        await self._updateSocialSDK(linked_users, activity)


    async def _updateIPC(self, is_enabled: bool, activity: dict[str, Any] | None) -> None:
        """
        同じ PC で起動している Discord に、IPC でアクティビティを設定する

        Args:
            is_enabled (bool): IPC での表示が有効か (誰も視聴していないか設定で無効なら False)
            activity (dict[str, Any] | None): 設定するアクティビティ
        """

        # 誰も視聴していないか設定で無効になっている場合、接続中であれば切断してアクティビティを消去する
        if is_enabled is False:
            if self._client.is_connected is True:
                await self._client.close()
            return

        # 未接続なら接続を試みる (Discord が起動していなければ次回に持ち越す)
        ## 再接続した場合、Discord 側のアクティビティは消えているため、前回の送信内容をリセットして必ず送り直す
        if self._client.is_connected is False:
            if await self._client.connect() is False:
                return
            self._last_activity = None

        # 前回送信した内容から変化があったときだけ送信する
        if activity != self._last_activity and await self._client.setActivity(activity) is True:
            self._last_activity = activity


    async def _updateSocialSDK(self, linked_users: list[User], activity: dict[str, Any] | None) -> None:
        """
        Discord アカウントと連携済みのユーザーごとに、Discord Social SDK でアクティビティを設定する

        Args:
            linked_users (list[User]): アクティビティを表示する、Discord アカウントと連携済みのユーザー
            activity (dict[str, Any] | None): 設定するアクティビティ
        """

        # 表示対象でなくなったユーザーのクライアントは、接続を閉じてアクティビティを消去する
        linked_user_ids = {user.id for user in linked_users}
        for user_id in list(self._social_sdk_clients):
            if user_id not in linked_user_ids:
                await self._social_sdk_clients.pop(user_id).close()

        # Discord Social SDK を読み込めなければ、トークンを更新しても表示できないため何もしない
        if len(linked_users) > 0 and DiscordSocialSDKClient.isAvailable() is False:
            return

        for user in linked_users:
            client = self._social_sdk_clients.get(user.id)

            # 未接続なら、アクセストークンを更新してから接続する
            ## 視聴を始めるたびに接続し直すため、有効期限 (7 日) を気にせず毎回ここで更新すれば足りる
            if client is None:
                try:
                    await user.requestDiscordToken({'grant_type': 'refresh_token', 'refresh_token': user.discord_refresh_token or ''})
                except Exception as ex:
                    logging.warning(f'[DiscordRichPresenceTask] Failed to refresh Discord access token. [user_id: {user.id}] ({ex})')
                    continue
                client = DiscordSocialSDKClient()
                if user.discord_access_token is None or await client.connect(user.discord_access_token) is False:
                    continue
                self._social_sdk_clients[user.id] = client

            # 接続が完了していれば送信する (完了していなければ次回に持ち越す)
            if await client.isReady() is True:
                await client.setActivity(activity)


    async def _getActivity(self, live_stream: LiveStream) -> dict[str, Any] | None:
        """
        視聴中のライブストリームから、Discord に設定するアクティビティを生成する

        Args:
            live_stream (LiveStream): 視聴中のライブストリーム

        Returns:
            dict[str, Any] | None: アクティビティ (チャンネル情報が見つからない場合は None)
        """

        channel = await Channel.filter(display_channel_id=live_stream.display_channel_id).first()
        if channel is None:
            return None

        # 現在放送中の番組と、同梱されている局ロゴを取得する
        program_present, _ = await channel.getCurrentAndNextProgram()
        logo_path = await channel.getBundledLogoFilePath()

        # 同梱の局ロゴがあれば大きい画像に局ロゴ、小さい画像に KonomiTV のアイコンを表示する
        ## ロゴのファイル名には全角文字を含むものがあるため、URL エンコードしておく
        if logo_path is not None:
            assets = {
                'large_image': self.LOGO_BASE_URL + quote(logo_path.relative_to(LOGO_DIR).as_posix()),
                'large_text': channel.name,
                'small_image': self.KONOMITV_ICON_URL,
                'small_text': 'KonomiTV',
            }
        # 同梱の局ロゴがなければ、大きい画像に KonomiTV のアイコンを表示する
        else:
            assets = {
                'large_image': self.KONOMITV_ICON_URL,
                'large_text': 'KonomiTV',
            }

        # アクティビティを生成する
        ## type: 3 は「Watching (視聴中)」を表す
        ## details / state は 2 〜 128 文字である必要があるため、切り詰めた上で 2 文字未満なら空白で埋める
        activity: dict[str, Any] = {
            'type': 3,
            'details': (program_present.title if program_present is not None else channel.name)[:128].ljust(2),
            'state': channel.name[:128].ljust(2),
            'assets': assets,
        }

        # 番組情報があれば、番組の開始・終了時刻 (UNIX ミリ秒) を設定して経過時間・残り時間を表示する
        if program_present is not None:
            activity['timestamps'] = {
                'start': int(program_present.start_time.timestamp() * 1000),
                'end': int(program_present.end_time.timestamp() * 1000),
            }

        return activity
