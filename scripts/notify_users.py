import os
import psycopg2
import psycopg2.extras
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(filename='C:/Users/cutek/OneDrive/Documents/bootcamp/LibraryPractice/scripts/notify_users.log',
                    level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection():
    return psycopg2.connect(
        dbname='Library',
        user='postgres',
        password='postgres',
        host='localhost',
        port=5432
    )


def fetch_due_books():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    due_date = datetime.now().date() + timedelta(days=2)
    cur.execute("""
        SELECT u."emailId" AS email, b.title, bd."dueDate" 
        FROM "BorrowingDetails" bd
        JOIN "BorrowingActivity" ba ON bd."activityId" = ba."activityId"
        JOIN "Users" u ON ba."userId" = u.id
        JOIN "Books" b ON bd."bookId" = b.id
        WHERE bd."dueDate" = %s
    """, (due_date,))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def log_email_sent(recipient, subject, body):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO "sEmailH" ("dateTime", "email", "subject", "body")
        VALUES (CURRENT_TIMESTAMP, %s, %s, %s)
    """, (recipient, subject, body))
    conn.commit()
    cur.close()
    conn.close()


def send_email(recipient, books_due):
    sender_email = os.getenv('EMAIL_USER')
    sender_password = os.getenv('EMAIL_PASS')

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient
    message["Subject"] = "Library Book Due Reminder"

    body = "Dear User,\n\nThis is a reminder that the following books are due in 2 days:\n"
    for book in books_due:
        body += f"- {book['title']} (Due on {book['dueDate']})\n"
    body += "\nPlease return them on time to avoid any late fees.\n\nThank you!"

    message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient, message.as_string())
            logging.info(f"Email sent to {recipient}")
            log_email_sent(recipient, message["Subject"], body)
    except Exception as e:
        logging.error(f"Failed to send email to {recipient}: {e}")


def main():
    due_books = fetch_due_books()
    users_books = {}

    for row in due_books:
        email = row['email']
        if email not in users_books:
            users_books[email] = []
        users_books[email].append(row)

    for email, books_due in users_books.items():
        send_email(email, books_due)


if __name__ == "__main__":
    logging.info('Script started')
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    logging.info('Script finished')
