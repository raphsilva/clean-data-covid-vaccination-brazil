import git
import os
import shutil
from integrations.git.git_x import REMOTE
from SETUP import PATH_REPO


def clone():
    if os.path.isdir(PATH_REPO):
        shutil.rmtree(PATH_REPO)
    git.Repo.clone_from(REMOTE, PATH_REPO)


def commit_and_push():
    repo = git.Repo(PATH_REPO)
    repo.git.add(".")
    repo.index.commit("Automatic update.")
    origin = repo.remote(name="origin")
    origin.push()
