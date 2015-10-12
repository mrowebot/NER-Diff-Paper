__author__ = 'rowem'

class Post:

    # def __init__(self, author, postid, forumid, date, content):
    #     self.author = author
    #     self.postid = postid
    #     self.forumid = forumid
    #     self.date = date
    #     self.content = content

    def __init__(self, author, postid, forumid, date):
        self.author = author
        self.postid = postid
        self.forumid = forumid
        self.date = date

    def __str__(self):
        return self.author \
               + " | " + self.postid \
               + " | " + self.forumid \
               + " | " + str(self.date) \
               + " | " + self.content \
               + " | " + self.annotations

    def addContent(self, content):
        self.content = content

    def addAnnotations(self, annotations):
        self.annotations = annotations
