const stateProduct = {
  validate: (application) => {
    // 12. unsupported state fails STATE_PRODUCT_001
    // For demonstration, let's assume 'NY' is the only supported state
    if (application.state && application.state !== "NY") {
      return {
        status: "FAIL",
        messages: [
          `STATE_PRODUCT_001: State ${application.state} is not supported.`,
        ],
      };
    }
    return { status: "PASS", messages: [] };
  },
};

module.exports = stateProduct;
