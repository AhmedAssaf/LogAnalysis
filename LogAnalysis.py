
from LogAnalysisDB import get_articles, get_authors ,get_requests_error

def main():
    """getting data from DB"""
    articles = get_articles()
    authors = get_authors()
    errors = get_requests_error()

    file = open("logAnalysisOutput.txt","w+")
    file.write('1. What are the most popular three articles of all time?.\n')
    for article in articles:
        file.write('"'+article[0]+'" -- '+str(article[1])+' views \n' )

    file.write('\n2. Who are the most popular article authors of all time?.\n')
    for author in authors:
        file.write(author[0] +' -- '+str(author[1])+' views \n' )

    file.write('\n3. On which days did more than 1% of requests lead to errors?.\n')
    for error in errors:
        file.write(error[0].strftime('%B %d,%Y') +' -- '+str(error[1])+' % errors \n' )
    
    file.close()

if __name__ == '__main__':
    main()
      
