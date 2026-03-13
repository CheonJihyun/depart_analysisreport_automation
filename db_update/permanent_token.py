import requests


def exchange_token_to_long_lived(app_id, app_secret, short_lived_token):
    """
    Short-lived 토큰을 Long-lived 토큰(60일)으로 교환

    Parameters:
    - app_id: Meta 앱 ID
    - app_secret: Meta 앱 시크릿
    - short_lived_token: 현재 가지고 있는 short-lived 액세스 토큰

    Returns:
    - dict: 응답 데이터 (access_token, token_type, expires_in 포함)
    """

    url = "https://graph.facebook.com/v18.0/oauth/access_token"

    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": short_lived_token,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # HTTP 에러 체크

        data = response.json()

        if "access_token" in data:
            print("토큰 변환 완료")
            print(f"새로운 Long-lived 토큰: {data['access_token']}")
            print(f"토큰 타입: {data.get('token_type', 'bearer')}")
            print(
                f"유효기간: {data.get('expires_in', 'N/A')}초 (약 {data.get('expires_in', 0) // 86400}일)"
            )
            return data
        else:
            print("토큰 교환 실패")
            print(f"응답: {data}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"오류: {e}")
        if hasattr(e.response, "text"):
            print(f"에러 상세: {e.response.text}")
        return None


# 사용 예시
if __name__ == "__main__":
    # 기존 토큰 정보 입력
    APP_ID = "793452136469892"
    APP_SECRET = "93c2766fd2a15b7f7997f4586085a4ac"
    SHORT_LIVED_TOKEN = "EAALRoZCi9lYQBQydoMwrXVmZCqVitjW8OpKKoSbX52cfwFWUR2bo4fBvAkXvzZCNAUQualy67aOKfrObqmQ1paq4ctNaJ0s8FcJx6aDUlj3oHOD2UwrjKr17i1bZBzgdvXaBfkygZA8BQe5bxqRhvFKePrcdANJ52TZBzup4SdhPYArNBOSml894hXpvkRR2IyarRYSbmQO1puI5pAZAVOZANT8vITwIwqrEAevAZC0NSIrqZAoRJeuYny57e3WxSQweZBTZCHZAZCDHAw5flVmUgLO9MxvwZDZD"

    result = exchange_token_to_long_lived(APP_ID, APP_SECRET, SHORT_LIVED_TOKEN)

    if result:
        # 생성된 장기 토큰을 파일에 저장
        with open("long_lived_token.txt", "w") as f:
            f.write(result["access_token"])
        print("'long_lived_token.txt' 파일에 토큰 저장 완료")
