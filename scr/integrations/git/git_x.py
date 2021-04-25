import json

reponame = json.loads(open('integrations/git/credentials.json').read())['repository']
username = json.loads(open('integrations/git/credentials.json').read())['username']
password = json.loads(open('integrations/git/credentials.json').read())['token']

REMOTE = f"https://{username}:{password}@github.com/{username}/{reponame}"


