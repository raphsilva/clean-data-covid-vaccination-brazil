import json

reponame = json.loads(open('integrations/git/credentials.json').read())['repository']
owner = json.loads(open('integrations/git/credentials.json').read())['owner']
username = json.loads(open('integrations/git/credentials.json').read())['username']
useremail = json.loads(open('integrations/git/credentials.json').read())['useremail']
password = json.loads(open('integrations/git/credentials.json').read())['token']

REMOTE = f"https://{username}:{password}@github.com/{owner}/{reponame}"
