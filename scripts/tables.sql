-- Split the Page column of the flat train data

DROP TABLE IF EXISTS train_flat_split;

CREATE TABLE train_flat_split AS (
  SELECT "Page" as page,
         reverse(split_part(reverse("Page"), '_', 3)) AS project,
         reverse(split_part(reverse("Page"), '_', 2)) AS access,
         reverse(split_part(reverse("Page"), '_', 1)) AS agent,
         "Date" AS date, "Visits" AS visits
    FROM train_flat
);
