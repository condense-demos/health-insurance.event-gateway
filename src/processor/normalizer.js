const { calculateAge } = require("../utils/date");
const { parseCurrencyString } = require("../utils/number");

const normalizeTobacco = (application) => {
  if (application && typeof application.tobaccoUse === "string") {
    application.tobaccoUse = application.tobaccoUse.toLowerCase() === "yes";
  }
  return application;
};

const normalizeCurrencyString = (application) => {
  if (application && typeof application.income === "string") {
    application.income = parseCurrencyString(application.income);
  }
  if (application && typeof application.coverageAmount === "string") {
    application.coverageAmount = parseCurrencyString(
      application.coverageAmount,
    );
  }
  return application;
};

const calculateAgeForApplication = (application) => {
  if (application && application.dateOfBirth) {
    application.age = calculateAge(application.dateOfBirth);
  }
  return application;
};

module.exports = {
  normalizeTobacco,
  normalizeCurrencyString,
  calculateAge: calculateAgeForApplication,
};
