Hello! This is a project that I thought of watching YouTube videos (usually ones related to finance) and seeing in the comments sections comments that were obviously fake. I
noticed this same style of spam messages across multiple channels, so I wondered if there's a way to build a spam detection model to classify these comments. It would create a more 
positive experience for both creators and viewers if these types of comments were removed, so I want to make that change. 

One of the difficulties I encountered during this process was figuring out how to not only get the top comments from a comments section, but also getting the replies within the
comment thread. I've noticed that many spam messages also have several spam messages as replies, so if I can get comments in replies then I can include that in my spam detection
algorithm. A potential avenue could be to specify that comments that are replies to another comment have an increased likelihood of being spam.
