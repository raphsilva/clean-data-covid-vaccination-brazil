import os
import shutil

import git

from SETUP import PATH_REPO
from integrations.git.git_x import REMOTE
from integrations.git.git_x import username, useremail

def clone_repository():
    if os.path.isdir(PATH_REPO):
        shutil.rmtree(PATH_REPO)
    git.Repo.clone_from(REMOTE, PATH_REPO)


def commit_and_push(msg='Update'):
    repo = git.Repo(PATH_REPO)
    repo.git.add(".")
    repo.config_writer().set_value("user", "name", username).release()
    repo.config_writer().set_value("user", "email", useremail).release()
    repo.index.commit(msg)
    origin = repo.remote(name="origin")
    origin.push()
