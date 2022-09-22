package pkg

import (
	"crypto/rand"
	"math/big"
)

func RandomCharacters(length int) (string, error) {
	randomString := ""

	possibleCharacters := []string{
		"a",
		"b",
		"c",
		"d",
		"e",
		"f",
		"g",
		"h",
		"i",
		"j",
		"k",
		"l",
		"m",
		"n",
		"o",
		"p",
		"q",
		"r",
		"s",
		"t",
		"u",
		"v",
		"w",
		"x",
		"y",
		"z",
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"H",
		"I",
		"J",
		"K",
		"L",
		"M",
		"N",
		"O",
		"P",
		"Q",
		"R",
		"S",
		"T",
		"U",
		"V",
		"W",
		"X",
		"Y",
		"Z",
		"1",
		"2",
		"3",
		"4",
		"5",
		"6",
		"7",
		"8",
		"9",
	}

	for i := 0; i < length; i++ {
		nBig, err := rand.Int(rand.Reader, big.NewInt(61))
		if err != nil {
			return "", err
		}
		randomInt := int(nBig.Int64())

		randomString = randomString + possibleCharacters[randomInt]
	}

	return randomString, nil
}
