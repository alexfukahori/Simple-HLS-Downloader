# @Author Alex Douglas Fukahori
# TODO list:
# - Better threads control
# - There are some synchronization issue when download live stream
# - Create documentation
# - Create requirement.txt

import errno
import os
import sys
import getopt
import socket
import wget
import ssl
import m3u8
from urlparse import urlparse
import threading
from Queue import Queue
import hashlib
import time
import atexit

live=False
duration=1
m3u8_files=[]
live_playlists=[]

class QueueNode:
	def __init__(self, segment, clear_net_location, playlist_url, dirname, tmp_basename, duration):
		self.segment = segment
		self.clear_net_location = clear_net_location
		self.playlist_url = playlist_url
		self.dirname = dirname
		self.tmp_basename=tmp_basename
		self.duration=duration

class Duration_Obj:
	def __init__(self):
		self.duration = 0

def md5(fname):
	hash_md5 = hashlib.md5()
	with open(fname, "rb") as f:
		for chunk in iter(lambda: f.read(4096), b""):
			hash_md5.update(chunk)
	return hash_md5.hexdigest()

def mkdir_p(path):
	if path == '':
		return
	try:
		os.makedirs(path)
	except OSError as exc:
		if exc.errno == errno.EEXIST and os.path.isdir(path):
			pass
		else:
			raise

def remove_first_char(path):
	while(path[0] == '/'):
		path=path[1:]
	return path

def download_file(name, url):
	print 'Downloading ' + url
	##### requests
	return requests.get(url)
	##### finish requests

def create_file(name, url):
	if os.path.exists(name):
		print 'Directory or file \'' + name + '\' already exists.'
		return False
	ssl._create_default_https_context = ssl._create_unverified_context
	print '\nDownloading ' + url
	wget.download(url, name)
	return True

def download_tag(tag, clear_net_location, curdir):
		dirname = curdir #+ os.path.dirname(tag.uri)
		if dirname == '':
			tag_uri=tag.uri
		else:
			tag_uri = dirname + '/' + tag.uri
		mkdir_p(os.path.dirname(tag.uri))

		create_file(tag_uri, clear_net_location + '/' + tag_uri)
		return tag_uri

def download_segment(segment, clear_net_location, file_to_append, curdir):
	dirname = curdir #+ os.path.dirname(segment.uri)
	if dirname == '':
		segment_uri=segment.uri
	else:
		segment_uri = dirname + '/' + segment.uri

	mkdir_p(os.path.dirname(segment.uri))

	created = create_file(segment_uri, clear_net_location + '/' + segment_uri)
	nl2Add=''
	print file_to_append
	if created == True:
		f=open(file_to_append,'rb+')
		f.seek(-1,2)
		lastChar=f.read()
		if lastChar!='\n':
			nl2Add='\n'
			f.close()
		f=open(file_to_append,'a+')
		f.write(nl2Add+str(segment))
		f.close()
	return created

def thread_manage(playlist, clear_net_location, curdir, tag_uri, playlist_curdir):
		tag_uri = download_tag(playlist, clear_net_location, curdir)
		thread1 = threading.Thread(target = download_from_playlist, args = [tag_uri, clear_net_location, playlist_curdir])
		thread1.start()

def download_from_playlist(playlist_url, clear_net_location, curdir):
	global live
	global duration
	global m3u8_files
	global live_playlists
	m3u8_obj = m3u8.load(playlist_url)  # this could also be an absolute filename
	tag_uri=''

	for segment in m3u8_obj.segments:
		thread1 = threading.Thread(target = download_tag, args=[segment, clear_net_location, curdir])
		thread1.start()

	for key in m3u8_obj.keys:
		try:
			if key.uri:
				thread1 = threading.Thread(target = download_tag, args = [key, clear_net_location, os.path.dirname(segment.uri)])
				thread1.start()
		except:
			continue

	for media in m3u8_obj.media:
		if media.uri == None:
			continue
		tag_curdir=os.path.dirname(media.uri)
		thread1 = threading.Thread(target = thread_manage, args = [media, clear_net_location, curdir, tag_uri, tag_curdir])
		thread1.start()
	
	for playlist in m3u8_obj.playlists:
		playlist_curdir=os.path.dirname(playlist.uri)
		thread1 = threading.Thread(target = thread_manage, args = [playlist, clear_net_location, curdir, tag_uri, playlist_curdir])
		thread1.start()

	###### if Live
	if m3u8_obj.segments and live == True:
		dur = Duration_Obj()
		dur.duration= m3u8_obj.target_duration * len(m3u8_obj.segments)
		tmp_basename = 'tmp/'+os.path.basename(playlist_url)
		m3u8_files.append(tmp_basename)
		live_playlists.append(playlist_url)

		mkdir_p('tmp')
		wait = (m3u8_obj.target_duration * len(m3u8_obj.segments)) / 4
		queu = Queue()
		threadQueue=threading.Thread(target=consume_queue, args=(queu,))
		threadQueue.setDaemon(True)
		threadQueue.start()
		while dur.duration < duration:
			time.sleep(wait)
			if os.path.exists(tmp_basename):
				os.remove(tmp_basename)
			ssl._create_default_https_context = ssl._create_unverified_context
			print '\nDownloading ' + clear_net_location + '/' + playlist_url
			wget.download(clear_net_location + '/' + playlist_url, tmp_basename)
			if md5(playlist_url) == md5(tmp_basename):
				continue
			to_append = m3u8.load(tmp_basename)
			for segment in to_append.segments:
				queu.put(QueueNode(segment,clear_net_location,playlist_url,os.path.dirname(segment.uri),tmp_basename,dur))
				print 'Thread: ' + threading.currentThread().getName() + ' Downloaded ' + str(dur.duration) + ' seconds'
			queu.join()
			# wait = (to_append.target_duration * len(to_append.segments)) / 2
			wait = 1

def consume_queue(q):
	while True:
		pop_obj = q.get()
		print threading.currentThread().getName() + ' ' + str(q.qsize())
		if not hasattr(pop_obj,'clear_net_location'):
			continue
		created = download_segment(pop_obj.segment, pop_obj.clear_net_location, pop_obj.playlist_url, pop_obj.dirname)
		if created == True:
				pop_obj.duration.duration += pop_obj.segment.duration
		q.task_done()
		print threading.currentThread().getName() + ' ' + str(q.qsize())

def exit_handler():
	global m3u8_files
	global live_playlists
	for x in m3u8_files:
		if os.path.exists(x):
			os.remove(x)
	for y in live_playlists:
		f=open(y,'a+')
		f.write('\n'+'#EXT-X-ENDLIST')
		f.close()
	try:
		if os.path.exists('tmp'):
			os.rmdir('tmp')
	except:
		pass


def print_usage():
	print 'Usage:'
	print sys.argv[0] + ' [-h] [-l] [-d duration] url'
	print '-l: Used for live stream download'
	print '-d: Download duration for live stream in seconds'
	print '-r: Use resolved DNS IP to speed up downloads'

def main():
	global live
	global duration
	resolved=False
	if len(sys.argv) < 2:
		print_usage()
		sys.exit(2)
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'rhd:l', ['help', 'duration='])
	except getopt.GetoptError as err:
		print str(err)  # will print something like "option -a not recognized"
		print_usage()
		sys.exit(2)

	for o, a in opts:
		if o == '-l':
			live = True
		elif o in ('-h', '--help'):
			print_usage()
			sys.exit()
		elif o in ('-d', '--duration'):
			duration = int(a)
		elif o in ('-r', '--resolved'):
			resolved = True
		else:
			assert False, 'unhandled option'

	url=args[0]
	path=urlparse(url)
	if resolved == True:
		host_ip = socket.gethostbyname(path.netloc)
	else:
		host_ip = path.netloc
	port = (':' + path.port) if path.port else ''
	os_path=remove_first_char(path.path)
	curdir=os.path.dirname(os_path)
	clear_net_location = path.scheme + '://' + host_ip + port + '/' + curdir
	print clear_net_location




	# mkdir_p(curdir)
	curdir=''
	os_path=os.path.basename(path.path)


	atexit.register(exit_handler)
	create_file(os.path.basename(path.path), url)

	thread_main = threading.Thread(target = download_from_playlist, args=[os_path, clear_net_location, curdir])
	thread_main.start()
	thread_main.join()

if __name__ == "__main__":
	main()
