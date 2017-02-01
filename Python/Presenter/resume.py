from heapq import *
def resume(self, lists):
	"""
	:type lists: heap
	:rtype: int
	When user click resume button, return the EIN that need to be processed. As during the pause, some other tasks maybe inserted for execution, so here we just pop up the front element in the heap.
	"""
	val, EIN = list[0]
	return EIN