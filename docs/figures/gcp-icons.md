# Google Cloud icons in the RTMSF diagram

The Google Cloud symbols embedded in `gcp-deployment.svg` come from Google's [official product icon library](https://cloud.google.com/icons), retrieved October 21, 2026. The library links the [core product icons package](https://services.google.com/fh/files/misc/core-products-icons.zip), [category icons package](https://services.google.com/fh/files/misc/category-icons.zip), and [Product icons overview](https://services.google.com/fh/files/misc/google-cloud-product-icons.pdf), updated May 2026.

The overview distinguishes “Unique core product icons (4-color)” from “General product category icons (2-color)” (page 3). Cloud Storage and Compute Engine have dedicated core icons (page 4). Pub/Sub and Dataproc Metastore both use the Data Analytics category icon (page 13). Identity and Access Management uses the Security and Identity category icon (page 28); the service-account/ADC card uses that category symbol with an explicit label. These category symbols are not product-specific Pub/Sub, Metastore or service-account logos.

The overview says legacy console icons “should not be used as of 2026” (page 33). Although the library still links a legacy package with distinct older Pub/Sub and Dataproc Metastore symbols, the diagram uses the current core/category system. The Metastore card represents Dataproc Metastore, not a Dataproc compute cluster.

Icons keep their original shapes, colors, orientation and proportions. Their square `0 0 512 512` viewboxes are scaled uniformly. SVG IDs and CSS classes are namespaced only to avoid collisions when embedding the source artwork. Worker modules, supervisor, Spark query witness and local evidence use generic diagram elements, with labels identifying their actual roles.

| Diagram component | Package | Original package file | SHA256 |
|---|---|---|---|
| Cloud Storage | Core products | `Unique Icons/Cloud Storage/SVG/Cloud_Storage-512-color.svg` | `df42fc4b652cb133a4150bea7988aa965651d2c52fdfd141b6e8a359552eec8e` |
| Compute Engine VM | Core products | `Unique Icons/Compute Engine/SVG/ComputeEngine-512-color-rgb.svg` | `5cf9e57e4f99e125e45f9db269f3d5d1fa0dfd187aebfe00f3f97b9caf488af2` |
| Pub/Sub and Dataproc Metastore | Categories | `Category Icons/Data Analytics/SVG/DataAnalytics-512-color.svg` | `b32c8b4702fb90e9669e841e0410687471b7ca97ad236527f6dad8e3d90c87d9` |
| IAM / service-account identity | Categories | `Category Icons/Security Identity/SVG/SecurityIdentity-512-color.svg` | `4e233138e1170653522e4261779d51bb8f1383255845aba893bbb2381fa48e0e` |

| Downloaded source | SHA256 |
|---|---|
| `core-products-icons.zip` | `6531a10f58bc599c24d9a455d81dd757c1a03c3c43da9cddf639b859c1c1eece` |
| `category-icons.zip` | `e5bc3abd3527dc2500e9bff7f15870783e2c764129c49b7cd4c1b4e105345002` |
| `google-cloud-product-icons.pdf` | `3cea6ac3abf8183462addffc2e3533a4c75f681f91981a2061131cbdc0c800a0` |
