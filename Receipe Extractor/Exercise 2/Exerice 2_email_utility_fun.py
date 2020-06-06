
#from jinja2 import Environment
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.header import Header
from email.MIMEBase import MIMEBase
from email import encoders
import base64
import sys

def body_table(to, sender, subject,message):
   
    #you = ['abc@searshc.com', 'bcy@searshc.com', 'xyz@searshc.com', 'xml@searshc.com', 'meera.laxman@searshc.com']
    you = [x for x in to.split(':')]
# Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = "".join(you)

#creating the body of the message in HTML version
# Create the plain-text and HTML version of your message
    html = """\
        <html>
          <head>
	  <style>
 	   p.serif{font-family: "Times New Roman", Times, serif;}
	   table {
	    font-family: "Times New Roman", Times, serif;
	    border-collapse: collapse;
	    width: 30%;
		}	

	  table, td, th {
		    border: 1px solid black;
		    text-align: left;
		    padding: 4px;
			}

	  tr:nth-child(even) {
		    background-color: #dddddd;
			     }
          </style>
          </head>
	  <body>
	  <p class = "serif"><font size ="3">Hi Team,<br>
	  <br>Following are the updates on today's Task Run:<br>
	  <h4 style="color:blue;"><u>"""+str(message)+"""&nbsp"""+"""</u><br></h4>
	  </p>

          </body>
        </html>
        """




# Turn these into plain/html MIMEText objects
    #part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

# Add HTML/plain-text parts to MIMEMultipart message
# The email client will try to render the last part first
    #msg.attach(part1)
    msg.attach(part2)

# Send the message via local SMTP server.
    s = smtplib.SMTP('localhost')
# sendmail function takes 3 arguments: sender's address, recipient's address
# and message to send - here it is sent as one string.
    s.sendmail(sender, you, msg.as_string())
    s.quit()


