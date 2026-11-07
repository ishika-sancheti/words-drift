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

fileOrFolderPath = r"E:\movies\reddit\comments"
subreddits_list_path=r"C:\Users\Ishika\words-drift\data\subreddit_dictionary.json"
words_list_path=r"C:\Users\Ishika\words-drift\data\word_dictionary.json"

output_dir=Path(r"C:\Users\Ishika\words-drift\data\processed")
output_dir.mkdir(exist_ok=True)              #create a new dir if it doesnt exist
recursive=False

with open(subreddits_list_path) as sr_list:			#this will give us the list of subreddits in which the words are to be searched
	subreddits_list=json.load(sr_list)

with open(words_list_path) as w_list:				#search these words and add a count to each word
	words_list=json.load(w_list)

def processFile(path: str):    
	#should be reset at every file and not keeping adding up from prev file data 
	word_counter=defaultdict(int)
	total_post_counter=0
	subreddit_post_counter=defaultdict(int)    
	for word in words_list["Psychology"]:
		word_counter[word]=0 
	for subreddit in subreddits_list["Psychology"]:
		subreddit_post_counter[subreddit]=0

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
			if row["subreddit"] in subreddits_list["Psychology"]:		
				subreddit_post_counter[row["subreddit"]]+=1
				for word in words_list["Psychology"]:			#find the word from lists, in the body				
					if word in row["body"].lower():
						word_counter[word]+=1           					    #update the counter for the word in the resp subreddit		
			else:
				continue
		progressLog.logProgress("\n")
		word_df=pd.DataFrame.from_dict(word_counter, orient='index', columns=['count'])
		subreddit_df=pd.DataFrame.from_dict(subreddit_post_counter, orient='index', columns=['count'])
		file_name=Path(path).stem
		word_df.to_parquet(output_dir / f"{file_name}_word_count.parquet")
		subreddit_df.to_parquet(output_dir /  f"{file_name}_subreddit_count.parquet")
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