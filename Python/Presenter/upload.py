from heapq import *
def upload(self, lists, EIN, year, priority = 0):
	"""
	:type EIN: str
	:type year: int
	:type priority: int
	:type lists: heap
	:rtype: heap
    When user want to upload a new task to the system, we push that into the priority queue. 
	"""
    # As we want a max heap, so hear just use the reverse of the priority
	heappush(lists, (0 - priority, (EIN, year)))
	return lists