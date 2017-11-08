CREATE TABLE "sample" (
  id INT NOT NULL,
  s VARCHAR(125),
  i INT,
  CONSTRAINT "sample_pkey" PRIMARY KEY (id)
);

CREATE TABLE "inner" (
  id INT NOT NULL,
  text VARCHAR(125),
  sample_id INT,
  CONSTRAINT "inner_pkey" PRIMARY KEY (id)
);