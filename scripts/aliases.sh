#! /bin/bash

alias tag_version='git tag "$(python setup.py --version)"'
alias tag_push='git push --tags'