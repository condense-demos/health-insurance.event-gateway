const config = require("../../config");

const productAge = {
  validate: (application) => {
    // 11. age outside range fails PRODUCT_AGE_001
    if (application.age) {
      if (
        application.age < config.product.minAge ||
        application.age > config.product.maxAge
      ) {
        return {
          status: "FAIL",
          messages: [
            `PRODUCT_AGE_001: Age ${application.age} is outside the allowed range (${config.product.minAge}-${config.product.maxAge}).`,
          ],
        };
      }
    }
    return { status: "PASS", messages: [] };
  },
};

module.exports = productAge;
