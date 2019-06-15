# bd-project2019
project for BigData course

## Aim

The project shared with riccardo Soro is composed by two jobs both implemented using MapReduce hadoop model
and Spark Sql

## Data

Dataset is about books and rating.
It's compostd by 5 tables and contains 10k of books with more than 6milion of ratings

## Jobs

Jobs are two:

* 1.Find out which are the most rated authors (first 100 authors with most high rating)
* 2.Comparing most bookmarked books with their rating

https://github.com/zygmuntz/goodbooks-10k

1) restituire i 100 autori che hanno il rating medio piu alto
1.1)considerare che il campo autore può avere più autori quindi bisognerà fare uno split.
1.2) Non utilizzare il valore medio dei rating nella tabella Books.vcs ma calcolarlo dalla tabella rating

2) Trovare una relazione tra libri che molti utenti vogliono leggere e rating dei libri.
2.1) Trovare quanti utenti vogliono leggere ogni libro e stimare empiricamente quanto è tanto e quanto è poco (Soglia)
2.2) mettere a confronto col raing dei libri (precalcolato) i vari bookmarks
es libri che in molti vogliono leggere sono anche i piu alti ad esempio?
