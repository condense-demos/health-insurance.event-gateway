const completeness = require("./rules/completeness");
const consent = require("./rules/consent");
const financial = require("./rules/financial");
const productAge = require("./rules/productAge");
const stateProduct = require("./rules/stateProduct");

const rules = [completeness, consent, financial, productAge, stateProduct];

const validate = (application) => {
  let results = [];

  for (const rule of rules) {
    results.push(rule.validate(application));
  }

  // 13. validation aggregation precedence FAIL > WARNING > PASS
  const overallResult = {
    status: "PASS",
    messages: [],
  };

  for (const result of results) {
    if (result.status === "FAIL") {
      overallResult.status = "FAIL";
    }
    if (overallResult.status !== "FAIL" && result.status === "WARNING") {
      overallResult.status = "WARNING";
    }
    overallResult.messages = overallResult.messages.concat(result.messages);
  }

  return overallResult;
};

module.exports = {
  validate,
};
