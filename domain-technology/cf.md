## query id use domain

                f"https://radar.cloudflare.com/charts/ScansTable/fetch?domain={domain}&type=url&value={domain}",

## use id to get detail


https://radar.cloudflare.com/scan/10d805b3-9dc9-4351-9ea0-d25fd79f077b/technology

## submit domain to get id

```
    def deep_inspect(url,timeout=25,proxy=None,user_agent=None,cookie=None,headers={}):
        h={}
        if user_agent:
            h.update({"User-Agent": user_agent})
        else:
            h.update({"User-Agent": random.choice(Common_Variables.user_agents_list)})
        if cookie:
            h.update({"Cookie": cookie})
        h.update(headers)
        try:
            d= requests.Session().post('https://radar.cloudflare.com/scan?index=&_data=routes%2Fscan%2Findex',data={'url':url},headers=h,proxies=proxy,timeout=timeout).json()
            url_id=d['data']['result']['uuid']
            report_url='https://radar.cloudflare.com/scan/'+url_id
            while True:
                time.sleep(5)
                h={}
                if user_agent:
                    h.update({"User-Agent": user_agent})
                else:
                    h.update({"User-Agent": random.choice(Common_Variables.user_agents_list)})
                if cookie:
                    h.update({"Cookie": cookie})
                h.update(headers)
                d=requests.Session().get('https://radar.cloudflare.com/scan?index=&_data=routes%2Fscan%2Findex',data={'url':url},headers=h,proxies=proxy,timeout=timeout).json()
                if '"message":"OK"' in str(d):
                    break
            d.update({'report_url':report_url})
            return d
        except Exception as ex:
            return {}
```
