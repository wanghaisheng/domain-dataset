![image](https://github.com/user-attachments/assets/7e474275-f618-46aa-8e9e-510567f542e9)## query id use domain

                f"https://radar.cloudflare.com/charts/ScansTable/fetch?domain={domain}&type=url&value={domain}",

## use id to get detail


https://radar.cloudflare.com/scan/10d805b3-9dc9-4351-9ea0-d25fd79f077b/technology


https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56
https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56/cookies
https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56/dom
https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56/network
https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56/performance
https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56/security
https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56/summary
https://radar.cloudflare.com/scan/0054c017-7666-4f46-8cc4-d23dc8a15e56/technology



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
