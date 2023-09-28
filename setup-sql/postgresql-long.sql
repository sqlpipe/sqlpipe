-- \c mydb;

-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CREATE OR REPLACE FUNCTION rdm_latin_letters_static(num_chars integer) RETURNS text AS $$
-- DECLARE
--     result text[];
--     i integer := 0;
--     random_char integer;
-- BEGIN

--     FOR i IN 1..num_chars LOOP
--         -- Generate random Latin letter (A-Z, a-z)
--         random_char := 
--             CASE 
--                 WHEN random() < 0.5 THEN 65 + floor(random() * 26)::integer -- Uppercase A-Z (ASCII 65-90)
--                 ELSE 97 + floor(random() * 26)::integer -- Lowercase a-z (ASCII 97-122)
--             END;
--         result[i] = chr(random_char);
--     END LOOP;
--     RETURN array_to_string(result, '');
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION rdm_ascii(num_chars integer) RETURNS text AS $$
-- DECLARE
--     result text[];
--     i integer := 0;
--     random_char integer;
-- BEGIN

--     num_chars = ceil(random() * num_chars);

--     FOR i IN 1..num_chars LOOP
--         random_char := 32 + floor(random() * 95)::integer;
--         result[i] := chr(random_char);
--     END LOOP;
--     RETURN array_to_string(result, '');
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION rdm_unicode(num_chars integer) RETURNS text AS $$
-- DECLARE
--     result text[]; -- use an array instead of text
--     i integer := 0;
--     random_char integer;
--     range_choice integer;
-- BEGIN
--     num_chars = ceil(random() * num_chars);

--     FOR i IN 1..num_chars LOOP
--         range_choice := floor(random() * 100)::integer; 

--         CASE
--             WHEN range_choice < 3 THEN
--                 random_char := 128 + floor(random() * 128)::integer; -- Extended unicode range
--             WHEN range_choice < 4 THEN
--                 random_char := 256 + floor(random() * 128)::integer; -- Another extended unicode range
--             WHEN range_choice < 5 THEN
--                 random_char := 384 + floor(random() * 112)::integer; -- Another extended unicode range

--             WHEN range_choice < 10 THEN
--                 random_char := 1024 + floor(random() * 256)::integer; -- Cyrillic


--             WHEN range_choice < 20 THEN
--                 random_char := 19968 + floor(random() * 20992)::integer; -- Chinese Han characters

--             WHEN range_choice < 25 THEN
--                 random_char := 1536 + floor(random() * 256)::integer; -- Arabic

--             WHEN range_choice < 30 THEN
--                 random_char := 44032 + floor(random() * 11172)::integer; -- Korean Hangul Syllables


--             WHEN range_choice < 32 THEN
--                 random_char := 12352 + floor(random() * 96)::integer; -- Hiragana
--             WHEN range_choice < 34 THEN
--                 random_char := 12448 + floor(random() * 96)::integer; -- Katakana


--             WHEN range_choice < 35 THEN
--                 random_char := 880 + floor(random() * 128)::integer; -- Greek
--             WHEN range_choice < 36 THEN
--                 random_char := 1424 + floor(random() * 128)::integer; -- Hebrew
--             WHEN range_choice < 38 THEN
--                 random_char := 2304 + floor(random() * 128)::integer; -- Hindi (Devanagari script)
--             WHEN range_choice < 39 THEN
--                 random_char := 3584 + floor(random() * 128)::integer; -- Thai
--             WHEN range_choice < 40 THEN
--                 random_char := 8704 + floor(random() * 256)::integer; -- Mathematical Symbols Basic
--             WHEN range_choice < 41 THEN
--                 random_char := 8192 + floor(random() * 112)::integer; -- General Punctuation
--             WHEN range_choice < 42 THEN
--                 random_char := 9472 + floor(random() * 128)::integer; -- Box Drawing
--             WHEN range_choice < 43 THEN
--                 random_char := 9728 + floor(random() * 192)::integer; -- Dingbats
--             WHEN range_choice < 44 THEN
--                 random_char := 12288 + floor(random() * 64)::integer; -- CJK Symbols and Punctuation
--             WHEN range_choice < 45 THEN
--                 random_char := 7680 + floor(random() * 256)::integer; -- unicode Extended Additional
--             WHEN range_choice < 46 THEN
--                 random_char := 127872 + floor(random() * 256)::integer; -- Symbols for Legacy Computing
--             WHEN range_choice < 50 THEN
--                 random_char := 128512 + floor(random() * 207)::integer; -- Extended mojis
--             ELSE -- everything else should be basic unicode range
--                 random_char := 32 + floor(random() * 95)::integer; -- Basic unicode range
--         END CASE;

--         result[i] := chr(random_char); -- assign character to the array at position i
--     END LOOP;

--     RETURN array_to_string(result, ''); -- convert the array back to a string
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION rdm_ascii_static(num_chars integer) RETURNS text AS $$
-- DECLARE
--     result text[];
--     i integer := 0;
--     random_char integer;
-- BEGIN

--     FOR i IN 1..num_chars LOOP
--         random_char := 32 + floor(random() * 95)::integer;
--         result[i] := chr(random_char);
--     END LOOP;
--     RETURN array_to_string(result, '');
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION rdm_unicode_static(num_chars integer) RETURNS text AS $$
-- DECLARE
--     result text[]; -- use an array instead of text
--     i integer := 0;
--     random_char integer;
--     range_choice integer;
-- BEGIN

--     FOR i IN 1..num_chars LOOP
--         range_choice := floor(random() * 100)::integer; 

--         CASE
--             WHEN range_choice < 3 THEN
--                 random_char := 128 + floor(random() * 128)::integer; -- Extended unicode range
--             WHEN range_choice < 4 THEN
--                 random_char := 256 + floor(random() * 128)::integer; -- Another extended unicode range
--             WHEN range_choice < 5 THEN
--                 random_char := 384 + floor(random() * 112)::integer; -- Another extended unicode range

--             WHEN range_choice < 10 THEN
--                 random_char := 1024 + floor(random() * 256)::integer; -- Cyrillic


--             WHEN range_choice < 20 THEN
--                 random_char := 19968 + floor(random() * 20992)::integer; -- Chinese Han characters

--             WHEN range_choice < 25 THEN
--                 random_char := 1536 + floor(random() * 256)::integer; -- Arabic

--             WHEN range_choice < 30 THEN
--                 random_char := 44032 + floor(random() * 11172)::integer; -- Korean Hangul Syllables


--             WHEN range_choice < 32 THEN
--                 random_char := 12352 + floor(random() * 96)::integer; -- Hiragana
--             WHEN range_choice < 34 THEN
--                 random_char := 12448 + floor(random() * 96)::integer; -- Katakana


--             WHEN range_choice < 35 THEN
--                 random_char := 880 + floor(random() * 128)::integer; -- Greek
--             WHEN range_choice < 36 THEN
--                 random_char := 1424 + floor(random() * 128)::integer; -- Hebrew
--             WHEN range_choice < 38 THEN
--                 random_char := 2304 + floor(random() * 128)::integer; -- Hindi (Devanagari script)
--             WHEN range_choice < 39 THEN
--                 random_char := 3584 + floor(random() * 128)::integer; -- Thai
--             WHEN range_choice < 40 THEN
--                 random_char := 8704 + floor(random() * 256)::integer; -- Mathematical Symbols Basic
--             WHEN range_choice < 41 THEN
--                 random_char := 8192 + floor(random() * 112)::integer; -- General Punctuation
--             WHEN range_choice < 42 THEN
--                 random_char := 9472 + floor(random() * 128)::integer; -- Box Drawing
--             WHEN range_choice < 43 THEN
--                 random_char := 9728 + floor(random() * 192)::integer; -- Dingbats
--             WHEN range_choice < 44 THEN
--                 random_char := 12288 + floor(random() * 64)::integer; -- CJK Symbols and Punctuation
--             WHEN range_choice < 45 THEN
--                 random_char := 7680 + floor(random() * 256)::integer; -- unicode Extended Additional
--             WHEN range_choice < 46 THEN
--                 random_char := 127872 + floor(random() * 256)::integer; -- Symbols for Legacy Computing
--             WHEN range_choice < 50 THEN
--                 random_char := 128512 + floor(random() * 207)::integer; -- Extended mojis
--             ELSE -- everything else should be basic unicode range
--                 random_char := 32 + floor(random() * 95)::integer; -- Basic unicode range
--         END CASE;

--         result[i] := chr(random_char); -- assign character to the array at position i
--     END LOOP;

--     RETURN array_to_string(result, ''); -- convert the array back to a string
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION rdm_bit(num_bits INTEGER)
-- RETURNS varbit AS $$
-- DECLARE
--     i integer := 0;
--     bit_str text[];
-- BEGIN
--     FOR i IN 1..num_bits LOOP
--         bit_str[i] := (CASE WHEN random() < 0.5 THEN '0' ELSE '1' END);
--     END LOOP;

--     RETURN array_to_string(bit_str, '')::varbit;
-- END;
-- $$ LANGUAGE plpgsql;


-- CREATE OR REPLACE FUNCTION rdm_varbit(num_bits INTEGER)
-- RETURNS varbit AS $$
-- DECLARE
--     i integer := 0;
--     bit_str text[];
-- BEGIN

--     num_bits = ceil(random() * num_bits);

--     FOR i IN 1..num_bits LOOP
--         bit_str[i] := (CASE WHEN random() < 0.5 THEN '0' ELSE '1' END);
--     END LOOP;

--     RETURN array_to_string(bit_str, '')::varbit;
-- END;
-- $$ LANGUAGE plpgsql;


-- CREATE OR REPLACE FUNCTION rdm_decimal(total_digits INTEGER, decimal_digits INTEGER)
-- RETURNS NUMERIC AS $$
-- DECLARE
--     int_part INTEGER;
--     decimal_part NUMERIC;
--     max_value INTEGER;
--     factor NUMERIC;
-- BEGIN
--     total_digits = ceil(random() * total_digits);
--     decimal_digits = ceil(total_digits - random() * decimal_digits);

--     -- Calculate the integer part's maximum value
--     max_value := 10 ^ (total_digits - decimal_digits) - 1;

--     -- Generate a random integer part
--     int_part := FLOOR(random() * max_value);

--     -- Calculate the factor for the decimal part based on scale
--     factor := 10 ^ decimal_digits;

--     -- Generate a random decimal part
--     decimal_part := FLOOR(random() * factor) / factor;

--     -- Combine the integer and decimal parts
--     RETURN int_part + decimal_part;
-- END;
-- $$ LANGUAGE plpgsql;


-- CREATE TABLE long_table (
--     random_unicode_text text,
--     random_int4 int4,
--     random_float4 float4,
--     random_numeric numeric(10, 5),
--     random_timestamp timestamp,
--     random_bytea bytea,
--     random_unicode_jsonb jsonb,
--     random_unicode_xml xml
-- );

-- DO $$ 
-- DECLARE 
--     counter INTEGER := 0;
-- BEGIN 
--     WHILE counter < 100 LOOP
--         INSERT INTO complex_table (
--     random_unicode_text,
--     random_int4,
--     random_float4,
--     random_numeric,
--     random_timestamp,
--     random_bytea,
--     random_unicode_jsonb,
--     random_unicode_xml
--         )
--         VALUES 
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32))),
--         (rdm_unicode(32), floor(random() * 2147483647)::int4, random()::float4, rdm_decimal(10, 5), NOW() - '1 year'::interval * random(), decode(md5(random()::text), 'hex'), jsonb_build_object(rdm_unicode(32), rdm_unicode(32)), xmlelement(name foo, xmlattributes(rdm_unicode(32) as bar), rdm_unicode(32)))
--         ;
--         counter := counter + 1;
--     END LOOP;
-- END;
-- $$ LANGUAGE plpgsql;