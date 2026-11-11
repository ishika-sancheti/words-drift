import sys
version=sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
	raise RuntimeError("This script requires Python 3.10 or higher")
import os
from collections import defaultdict
from typing import Iterable

import json

from fileStreams import getFileJsonStream
from utils import FileProgressLog

import pandas as pd

from pathlib import Path

import re

fileOrFolderPath = r"E:\movies\reddit\comments"
subreddits_list_path=r"C:\Users\Ishika\words-drift\data\subreddit_dictionary.json"
words_list_path=r"C:\Users\Ishika\words-drift\data\word_dictionary.json"

output_dir=Path(r"C:\Users\Ishika\words-drift\data\processed")
output_dir.mkdir(exist_ok=True)              #create a new dir if it doesnt exist exist_ok=true
recursive=False

with open(subreddits_list_path) as sr_list:			#this will give us the list of subreddits in which the words are to be searched
	subreddits_list=json.load(sr_list)

with open(words_list_path) as w_list:				#search these words and add a count to each word
	words_list=json.load(w_list)

patterns={}
for category in words_list:
	patterns[category]={
		word:re.compile(r"\b" + re.escape(word) + r"\b")
		for word in words_list[category]
	}

def processFile(path: str):
	file_name=Path(path).stem
	if (output_dir/next(iter(words_list))/f"{file_name}_word_count.parquet").exists():
		print(f"Skipping {path}, already processed")
		return
	   
	category_dirs={}								#creating a separate dir for every category
	for category in words_list:
		category_dir=output_dir/category
		category_dir.mkdir(parents=True, exist_ok=True)
		category_dirs[category]=category_dir

	all_niche=set().union(*subreddits_list.values())
	sitewide_counter={c: {w: 0 for w in words_list[c]} for c in words_list}
	non_niche_seen=0
	sampled_total=0
	
	#counters should be reset at every file and not keeping adding up from prev
	total_post_counter=0   
	word_counter={}
	subreddit_post_counter={}
	for category in words_list:						
		word_counter[category]=defaultdict(int)
		subreddit_post_counter[category]=defaultdict(int)
		for word in words_list[category]:				             #initialising every word and subreddit count to zero
			word_counter[category][word]=0 
		for subreddit in subreddits_list[category]:
			subreddit_post_counter[category][subreddit]=0

	print(f"Processing file {path}")
	with open(path, "rb") as f:
		jsonStream=getFileJsonStream(path, f)
		if jsonStream is None:
			print(f"Skipping unknown file {path}")
			return
		progressLog=FileProgressLog(path, f)
		for row in jsonStream:
			progressLog.onRow()
			if row["body"]=="[deleted]":
				continue
			total_post_counter+=1
			subreddit=row["subreddit"].lower()
			for category in words_list:
				if subreddit in subreddits_list[category]:	
					subreddit_post_counter[category][subreddit]+=1
					body=row["body"].lower()
					for word, pattern in patterns[category].items():									#find the word from lists, in the body				
						if word in body and pattern.search(body):
							word_counter[category][word]+=1           		#update the counter for the word in the resp subreddit		
			if subreddit not in all_niche:
				non_niche_seen+=1
				if non_niche_seen%10==0:
					sampled_total+=1
					body=row["body"].lower()
					for category in patterns:
						for word, pattern in patterns[category].items():									#find the word from lists, in the body				
							if word in body and pattern.search(body):
								sitewide_counter[category][word]+=1
		
		progressLog.logProgress("\n")
		for category in words_list:
			word_df=pd.DataFrame.from_dict(word_counter[category], orient='index', columns=['count'])
			word_df["sitewide_count"]=pd.Series(sitewide_counter[category])
			word_df["sitewide_sampled"]=sampled_total
			subreddit_df=pd.DataFrame.from_dict(subreddit_post_counter[category], orient='index', columns=['count'])
			subreddit_df["total_comments"]=total_post_counter
			
			file_name=Path(path).stem
			word_df.to_parquet(category_dirs[category] / f"{file_name}_word_count.parquet")
			subreddit_df.to_parquet(category_dirs[category] / f"{file_name}_subreddit_count.parquet")
		return total_post_counter, subreddit_post_counter, word_counter

#iterating thru multiple files
def processFolder(path: str):
	fileIterator: Iterable[str]
	if recursive:
		def recursiveFileIterator():
			for root, dirs, files in os.walk(path):
				for file in files:
					yield os.path.join(root, file)
		fileIterator = recursiveFileIterator()
	else:
		fileIterator = sorted(os.listdir(path))
		fileIterator = (os.path.join(path, file) for file in fileIterator)
	
	for i, file in enumerate(fileIterator):
		if not file.endswith(".zst"):
			continue
		print(f"Processing file {i+1: 3} {file}")
		processFile(file)

def main():
	if os.path.isdir(fileOrFolderPath):
		processFolder(fileOrFolderPath)
	else:
		processFile(fileOrFolderPath)
	print("Done :>")
	#print("\n", "no of posts counted: ", total_post_counter, "\n", "no of posts in the psychology subreddit counted: ", subreddit_post_counter, "\n","no of times the word 'psychoanalysis' is mentioned in psychology subreddit: ", word_counter, "\n")

if __name__ == "__main__":
	main()