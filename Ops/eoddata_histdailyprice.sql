CREATE TABLE `histdailyprice6` (
  `Date` date NOT NULL,
  `Symbol` varchar(45) NOT NULL,
  `Exchange` varchar(45) NOT NULL,
  `Close` float DEFAULT NULL,
  `Open` float DEFAULT NULL,
  `High` float DEFAULT NULL,
  `Low` float DEFAULT NULL,
  `Volume` bigint DEFAULT NULL,
  `AdjClose` float DEFAULT NULL,
  PRIMARY KEY (`Date`,`Symbol`,`Exchange`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
