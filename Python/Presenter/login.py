def login(self, uid, pwd, pwdList):
	"""
	:type uid: string
	:type pwd: string
	:type pwdList: dictionary{id:pwd}
	:rtype: Boolean
	Find in pwdList for the uid, if the password corresponding to the uid is the same as pwd, return true; otherwise, false.
	"""
	if pwdList[uid] == pwd:
		return True 
	else:
		return False 