--
-- Split the Page column of the flat train data
--

DROP TABLE IF EXISTS train_flat_split;

CREATE TABLE train_flat_split AS (
  SELECT "Page" AS page,
         reverse(split_part(reverse("Page"), '_', 3)) AS project,
         reverse(split_part(reverse("Page"), '_', 2)) AS access,
         reverse(split_part(reverse("Page"), '_', 1)) AS agent,
         left("Page",
           -length(reverse(split_part(reverse("Page"), '_', 3)))
           -length(reverse(split_part(reverse("Page"), '_', 2)))
           -length(reverse(split_part(reverse("Page"), '_', 1)))
           - 3) AS name,
         date_part('dow', "Date") AS dow,
         date_part('month', "Date") AS month,
         "Date"::DATE AS date, "Visits" AS visits
    FROM train_flat
);

ALTER TABLE train_flat_split
  ADD PRIMARY KEY (page, date);
CREATE INDEX ON train_flat_split ("date");
CREATE INDEX ON train_flat_split (page);
CREATE INDEX ON train_flat_split (project);


--
-- Table of features (visits_lag#, project, access, and agent) and response value (visits)
--

DROP TABLE IF EXISTS xy;

CREATE TABLE xy AS (
  SELECT page, name, project,
         CASE WHEN access='all-access' THEN 0 WHEN access='desktop' THEN 1 ELSE 2 END AS access,
         CASE WHEN agent='all-agents' THEN 0 ELSE 1 END AS agent,
         dow, month, date::DATE, visits,
         lag(visits, 1) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag1,
         lag(visits, 2) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag2,
         lag(visits, 3) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag3,
         lag(visits, 4) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag4,
         lag(visits, 5) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag5,
         lag(visits, 6) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag6,
         lag(visits, 7) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag7,
         lag(visits, 8) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag8,
         lag(visits, 9) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag9,
         lag(visits, 10) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag10,
         lag(visits, 11) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag11,
         lag(visits, 12) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag12,
         lag(visits, 13) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag13,
         lag(visits, 14) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag14,
         lag(visits, 15) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag15,
         lag(visits, 16) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag16,
         lag(visits, 17) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag17,
         lag(visits, 18) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag18,
         lag(visits, 19) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag19,
         lag(visits, 20) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag20,
         lag(visits, 21) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag21,
         lag(visits, 22) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag22,
         lag(visits, 23) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag23,
         lag(visits, 24) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag24,
         lag(visits, 25) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag25,
         lag(visits, 26) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag26,
         lag(visits, 27) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag27,
         lag(visits, 28) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag28,
         lag(visits, 29) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag29,
         lag(visits, 30) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag30,
         lag(visits, 31) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag31,
         lag(visits, 32) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag32,
         lag(visits, 33) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag33,
         lag(visits, 34) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag34,
         lag(visits, 35) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag35,
         lag(visits, 36) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag36,
         lag(visits, 37) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag37,
         lag(visits, 38) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag38,
         lag(visits, 39) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag39,
         lag(visits, 40) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag40,
         lag(visits, 41) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag41,
         lag(visits, 42) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag42,
         lag(visits, 43) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag43,
         lag(visits, 44) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag44,
         lag(visits, 45) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag45,
         lag(visits, 46) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag46,
         lag(visits, 47) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag47,
         lag(visits, 48) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag48,
         lag(visits, 49) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag49,
         lag(visits, 50) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag50,
         lag(visits, 51) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag51,
         lag(visits, 52) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag52,
         lag(visits, 53) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag53,
         lag(visits, 54) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag54,
         lag(visits, 55) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag55,
         lag(visits, 56) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag56,
         lag(visits, 57) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag57,
         lag(visits, 58) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag58,
         lag(visits, 59) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag59,
         lag(visits, 60) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag60,
         lag(visits, 61) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag61,
         lag(visits, 62) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag62,
         lag(visits, 63) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag63,
         lag(visits, 64) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag64,
         lag(visits, 65) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag65,
         lag(visits, 66) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag66,
         lag(visits, 67) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag67,
         lag(visits, 68) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag68,
         lag(visits, 69) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag69,
         lag(visits, 70) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag70,
         lag(visits, 71) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag71,
         lag(visits, 72) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag72,
         lag(visits, 73) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag73,
         lag(visits, 74) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag74,
         lag(visits, 75) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag75,
         lag(visits, 76) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag76,
         lag(visits, 77) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag77,
         lag(visits, 78) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag78,
         lag(visits, 79) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag79,
         lag(visits, 80) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag80,
         lag(visits, 81) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag81,
         lag(visits, 82) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag82,
         lag(visits, 83) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag83,
         lag(visits, 84) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag84,
         lag(visits, 85) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag85,
         lag(visits, 86) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag86,
         lag(visits, 87) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag87,
         lag(visits, 88) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag88,
         lag(visits, 89) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag89,
         lag(visits, 90) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag90,
         lag(visits, 91) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag91,
         lag(visits, 92) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag92,
         lag(visits, 93) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag93,
         lag(visits, 94) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag94,
         lag(visits, 95) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag95,
         lag(visits, 96) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag96,
         lag(visits, 97) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag97,
         lag(visits, 98) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag98,
         lag(visits, 99) OVER (PARTITION BY page ORDER BY date ASC) AS visits_lag99
    FROM train_flat_split
);

ALTER TABLE xy
  ADD PRIMARY KEY (page, date);
CREATE INDEX ON xy ("date");
CREATE INDEX ON xy (page);
CREATE INDEX ON xy (project);
CREATE INDEX ON xy (agent);


--
-- Table of test features to predict response
--

DROP TABLE IF EXISTS testx;

CREATE TABLE testx AS (
  WITH dates AS (
     SELECT i::DATE AS date
       FROM generate_series('2017-01-01', '2017-03-01', '1 day'::INTERVAL) i
  )
     SELECT pages.page AS page,
            reverse(split_part(reverse(pages.page), '_', 3)) AS project,
            CASE WHEN reverse(split_part(reverse(pages.page), '_', 2))='all-access' THEN 0
                 WHEN reverse(split_part(reverse(pages.page), '_', 2))='desktop' THEN 1 ELSE 2 END AS access,
            CASE WHEN reverse(split_part(reverse(pages.page), '_', 1))='all-agents' THEN 0 ELSE 1 END AS agent,
            date_part('dow', pages.date) AS dow,
            date_part('month', pages.date) AS month,
            pages.date AS date,
            xy.visits AS visits_lag12,
            xy.visits_lag1 AS visits_lag13,
            xy.visits_lag2 AS visits_lag14,
            xy.visits_lag3 AS visits_lag15,
            xy.visits_lag4 AS visits_lag16,
            xy.visits_lag5 AS visits_lag17,
            xy.visits_lag6 AS visits_lag18,
            xy.visits_lag7 AS visits_lag19,
            xy.visits_lag8 AS visits_lag20,
            xy.visits_lag9 AS visits_lag21,
            xy.visits_lag10 AS visits_lag22,
            xy.visits_lag11 AS visits_lag23,
            xy.visits_lag12 AS visits_lag24,
            xy.visits_lag13 AS visits_lag25,
            xy.visits_lag14 AS visits_lag26,
            xy.visits_lag15 AS visits_lag27,
            xy.visits_lag16 AS visits_lag28,
            xy.visits_lag17 AS visits_lag29,
            xy.visits_lag18 AS visits_lag30,
            xy.visits_lag19 AS visits_lag31,
            xy.visits_lag20 AS visits_lag32,
            xy.visits_lag21 AS visits_lag33,
            xy.visits_lag22 AS visits_lag34,
            xy.visits_lag23 AS visits_lag35,
            xy.visits_lag24 AS visits_lag36,
            xy.visits_lag25 AS visits_lag37,
            xy.visits_lag26 AS visits_lag38,
            xy.visits_lag27 AS visits_lag39,
            xy.visits_lag28 AS visits_lag40,
            xy.visits_lag29 AS visits_lag41,
            xy.visits_lag30 AS visits_lag42,
            xy.visits_lag31 AS visits_lag43,
            xy.visits_lag32 AS visits_lag44,
            xy.visits_lag33 AS visits_lag45,
            xy.visits_lag34 AS visits_lag46,
            xy.visits_lag35 AS visits_lag47,
            xy.visits_lag36 AS visits_lag48,
            xy.visits_lag37 AS visits_lag49,
            xy.visits_lag38 AS visits_lag50,
            xy.visits_lag39 AS visits_lag51,
            xy.visits_lag40 AS visits_lag52,
            xy.visits_lag41 AS visits_lag53,
            xy.visits_lag42 AS visits_lag54,
            xy.visits_lag43 AS visits_lag55,
            xy.visits_lag44 AS visits_lag56,
            xy.visits_lag45 AS visits_lag57,
            xy.visits_lag46 AS visits_lag58,
            xy.visits_lag47 AS visits_lag59,
            xy.visits_lag48 AS visits_lag60,
            xy.visits_lag49 AS visits_lag61,
            xy.visits_lag50 AS visits_lag62,
            xy.visits_lag51 AS visits_lag63,
            xy.visits_lag52 AS visits_lag64,
            xy.visits_lag53 AS visits_lag65,
            xy.visits_lag54 AS visits_lag66,
            xy.visits_lag55 AS visits_lag67,
            xy.visits_lag56 AS visits_lag68,
            xy.visits_lag57 AS visits_lag69,
            xy.visits_lag58 AS visits_lag70,
            xy.visits_lag59 AS visits_lag71,
            xy.visits_lag60 AS visits_lag72,
            xy.visits_lag61 AS visits_lag73,
            xy.visits_lag62 AS visits_lag74,
            xy.visits_lag63 AS visits_lag75,
            xy.visits_lag64 AS visits_lag76,
            xy.visits_lag65 AS visits_lag77,
            xy.visits_lag66 AS visits_lag78,
            xy.visits_lag67 AS visits_lag79,
            xy.visits_lag68 AS visits_lag80,
            xy.visits_lag69 AS visits_lag81,
            xy.visits_lag70 AS visits_lag82,
            xy.visits_lag71 AS visits_lag83,
            xy.visits_lag72 AS visits_lag84,
            xy.visits_lag73 AS visits_lag85,
            xy.visits_lag74 AS visits_lag86,
            xy.visits_lag75 AS visits_lag87,
            xy.visits_lag76 AS visits_lag88,
            xy.visits_lag77 AS visits_lag89,
            xy.visits_lag78 AS visits_lag90,
            xy.visits_lag79 AS visits_lag91
       FROM (SELECT page, dates.date FROM (SELECT DISTINCT page FROM train_flat_split) a, dates) pages
  LEFT JOIN xy ON xy.page=pages.page AND xy.date=pages.date-12
);

ALTER TABLE testx
  ADD PRIMARY KEY (page, date);
CREATE INDEX ON testx ("date");
CREATE INDEX ON testx (page);
CREATE INDEX ON testx (project);
CREATE INDEX ON testx (access);
