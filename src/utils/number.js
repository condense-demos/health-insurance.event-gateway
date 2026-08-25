const parseCurrencyString = (currencyString) => {
  if (typeof currencyString !== "string") {
    return currencyString;
  }
  // Remove currency symbols, commas, and extra spaces, then parse as float
  return parseFloat(currencyString.replace(/[^0-9.-]+/g, ""));
};

module.exports = {
  parseCurrencyString,
};
