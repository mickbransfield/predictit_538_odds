#"C:\Users\Mick\Anaconda3\python.exe" "C:\Users\Mick\Documents\Python\Python\predictit_538_odds\predictit_538_presidential.py
#pause

#!/bin/bash
##########################################################################
# Navigate to the folder
# Auto commit the file changes (if any)
# The word count of the output is greater than 0, then changes is observed
##########################################################################

# Task 1: Navigate to the folder
cd "C:\Users\Mick\Documents\Python\Python\predictit_538_odds"

# Task 2: Git add, commit, push
if [[ $(git status --porcelain | wc -l) -gt 0 ]]; then 
  # echo Changes
  git add --all
  git commit -m "Auto Update | $(date +"%D %T")"
  git push origin master
fi