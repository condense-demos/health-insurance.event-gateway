const config = require("../../config");

const financial = {
  validate: (application) => {
    // 10. ratio > 5 causes FINANCIAL_001 warning
    if (application.income && application.coverageAmount) {
      const ratio = application.coverageAmount / application.income;
      if (ratio > config.financial.coverageIncomeRatioLimit) {
        return {
          status: "WARNING",
          messages: ["FINANCIAL_001: Coverage income ratio exceeds limit."],
        };
      }
    }
    return { status: "PASS", messages: [] };
  },
};

module.exports = financial;
