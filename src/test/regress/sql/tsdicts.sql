--Test text search dictionaries and configurations

-- Test ISpell dictionary with ispell affix file
CREATE TEXT SEARCH DICTIONARY ispell (
                        Template=ispell,
                        DictFile=ispell_sample,
                        AffFile=ispell_sample
);

SELECT ts_lexize('ispell', 'skies');
SELECT ts_lexize('ispell', 'bookings');
SELECT ts_lexize('ispell', 'booking');
SELECT ts_lexize('ispell', 'foot');
SELECT ts_lexize('ispell', 'foots');
SELECT ts_lexize('ispell', 'rebookings');
SELECT ts_lexize('ispell', 'rebooking');
SELECT ts_lexize('ispell', 'rebook');
SELECT ts_lexize('ispell', 'unbookings');
SELECT ts_lexize('ispell', 'unbooking');
SELECT ts_lexize('ispell', 'unbook');

SELECT ts_lexize('ispell', 'footklubber');
SELECT ts_lexize('ispell', 'footballklubber');
SELECT ts_lexize('ispell', 'ballyklubber');
SELECT ts_lexize('ispell', 'footballyklubber');

-- Test ISpell dictionary with hunspell affix file
CREATE TEXT SEARCH DICTIONARY hunspell (
                        Template=ispell,
                        DictFile=ispell_sample,
                        AffFile=hunspell_sample
);

SELECT ts_lexize('hunspell', 'skies');
SELECT ts_lexize('hunspell', 'bookings');
SELECT ts_lexize('hunspell', 'booking');
SELECT ts_lexize('hunspell', 'foot');
SELECT ts_lexize('hunspell', 'foots');
SELECT ts_lexize('hunspell', 'rebookings');
SELECT ts_lexize('hunspell', 'rebooking');
SELECT ts_lexize('hunspell', 'rebook');
SELECT ts_lexize('hunspell', 'unbookings');
SELECT ts_lexize('hunspell', 'unbooking');
SELECT ts_lexize('hunspell', 'unbook');

SELECT ts_lexize('hunspell', 'footklubber');
SELECT ts_lexize('hunspell', 'footballklubber');
SELECT ts_lexize('hunspell', 'ballyklubber');
SELECT ts_lexize('hunspell', 'footballyklubber');

-- Synonim dictionary
CREATE TEXT SEARCH DICTIONARY synonym (
						Template=synonym,
						Synonyms=synonym_sample
);

SELECT ts_lexize('synonym', 'PoStGrEs');
SELECT ts_lexize('synonym', 'Gogle');
SELECT ts_lexize('synonym', 'indices');

-- Create and simple test thesaurus dictionary
-- More tests in configuration checks because ts_lexize()
-- cannot pass more than one word to thesaurus.
CREATE TEXT SEARCH DICTIONARY thesaurus (
                        Template=thesaurus,
						DictFile=thesaurus_sample,
						Dictionary=english_stem
);

SELECT ts_lexize('thesaurus', 'one');

-- Test ispell dictionary in configuration
CREATE TEXT SEARCH CONFIGURATION ispell_tst (
						COPY=english
);

ALTER TEXT SEARCH CONFIGURATION ispell_tst ALTER MAPPING FOR
	word, numword, asciiword, hword, numhword, asciihword, hword_part, hword_numpart, hword_asciipart
	WITH ispell, english_stem;

SELECT to_tsvector('ispell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
SELECT to_tsquery('ispell_tst', 'footballklubber');
SELECT to_tsquery('ispell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test ispell dictionary with hunspell affix in configuration
CREATE TEXT SEARCH CONFIGURATION hunspell_tst (
						COPY=ispell_tst
);

ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING
	REPLACE ispell WITH hunspell;

SELECT to_tsvector('hunspell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
SELECT to_tsquery('hunspell_tst', 'footballklubber');
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:A & sky');

SELECT to_tsquery('hunspell_tst', 'footballyklubber:b ? sky');
SELECT phraseto_tsquery('hunspell_tst', 'footballyklubber sky');

-- Test synonym dictionary in configuration
CREATE TEXT SEARCH CONFIGURATION synonym_tst (
						COPY=english
);

ALTER TEXT SEARCH CONFIGURATION synonym_tst ALTER MAPPING FOR
	asciiword, hword_asciipart, asciihword
	WITH synonym, english_stem;

SELECT to_tsvector('synonym_tst', 'Postgresql is often called as postgres or pgsql and pronounced as postgre');
SELECT to_tsvector('synonym_tst', 'Most common mistake is to write Gogle instead of Google');
SELECT to_tsvector('synonym_tst', 'Indexes or indices - Which is right plural form of index?');
SELECT to_tsquery('synonym_tst', 'Index & indices');

-- test thesaurus in configuration
-- see thesaurus_sample.ths to understand 'odd' resulting tsvector
CREATE TEXT SEARCH CONFIGURATION thesaurus_tst (
						COPY=synonym_tst
);

ALTER TEXT SEARCH CONFIGURATION thesaurus_tst ALTER MAPPING FOR
	asciiword, hword_asciipart, asciihword
	WITH synonym, thesaurus, english_stem;

SELECT to_tsvector('thesaurus_tst', 'one postgres one two one two three one');
SELECT to_tsvector('thesaurus_tst', 'Supernovae star is very new star and usually called supernovae (abbrevation SN)');
SELECT to_tsvector('thesaurus_tst', 'Booking tickets is looking like a booking a tickets');
