import json

reponame = json.loads(open('integrations/git/credentials.json').read())['repository']
username = json.loads(open('integrations/git/credentials.json').read())['repo-user']
password = json.loads(open('integrations/git/credentials.json').read())['token']

REMOTE = f"https://{username}:{password}@github.com/{username}/{reponame}"


