
import APIClient from '@/services/APIClient';


/** Discord アカウントと連携するための認証 URL を表すインターフェイス */
export interface IDiscordAuthURL {
    authorization_url: string;
}


class Discord {

    /**
     * Discord アカウントと連携するための認証 URL を取得する
     * @returns 認証 URL or 認証 URL の取得に失敗した場合は null
     */
    static async fetchAuthorizationURL(): Promise<string | null> {

        // API リクエストを実行
        const response = await APIClient.get<IDiscordAuthURL>('/discord/auth');

        // エラー処理
        if (response.type === 'error') {
            switch (response.data.detail) {
                case 'Discord Social SDK is not installed': {
                    APIClient.showGenericError(response, 'Discord Social SDK がサーバーにインストールされていないため、連携できません。');
                    break;
                }
                default: {
                    APIClient.showGenericError(response, 'Discord アカウントとの連携用の認証 URL を取得できませんでした。');
                    break;
                }
            }
            return null;
        }

        return response.data.authorization_url;
    }


    /**
     * 現在ログイン中のユーザーアカウントに紐づく Discord アカウントとの連携を解除する
     * @returns 連携解除に成功した場合は true, 失敗した場合は false
     */
    static async logoutAccount(): Promise<boolean> {

        // API リクエストを実行
        const response = await APIClient.delete('/discord/logout');

        // エラー処理
        if (response.type === 'error') {
            APIClient.showGenericError(response, 'Discord アカウントとの連携を解除できませんでした。');
            return false;
        }

        return true;
    }
}

export default Discord;
