# AWS icons in the RTMSF diagram

The AWS service and resource symbols embedded in `aws-deployment.svg` come from the [AWS architecture icons](https://aws.amazon.com/architecture/icons/) library and its [July 31, 2026 icon package](https://d1.awsstatic.com/onedam/marketing-channels/website/public/shared/architecture-icon-release/Icon-package_07312026.5846e92413caa21490223536cc97f1269e44fa92.zip), retrieved October 21, 2026.

AWS's library page states: “We allow customers and partners to use these toolkits and assets to create architecture diagrams.” It also says: “You can also put icons in materials like whitepapers, presentations, data sheets, and posters.” The same page recommends checking that icons are up to date because third-party libraries may contain legacy sets.

The diagram uses dedicated service icons for S3 and ECR. ECS and Fargate retain separate official service symbols side by side; there is no invented combined ECS/Fargate logo. Both explicitly labeled SQS queues use the same official Queue resource icon. IAM task role, Glue Data Catalog and CloudWatch Logs use their precise official resource symbols rather than generic IAM, Glue or CloudWatch service icons. Worker modules use generic diagram elements.

Icons keep their original shapes, colors, orientation and proportions. Service SVGs from the package's `48` directories have `0 0 64 64` viewboxes; the selected resource SVGs have `0 0 48 48` viewboxes. Each is scaled uniformly using its original viewbox. SVG IDs are namespaced only to avoid collisions when embedding the source artwork.

| Diagram component | Original package file | SHA256 |
|---|---|---|
| Amazon S3 | `Architecture-Service-Icons_07312026/Arch_Storage/48/Arch_Amazon-Simple-Storage-Service_48.svg` | `bb0df5b7ca52da7323888ed588baefa1c68a23b5776f325bc8ec29825c185a31` |
| Amazon ECR | `Architecture-Service-Icons_07312026/Arch_Containers/48/Arch_Amazon-Elastic-Container-Registry_48.svg` | `bf829eab882165f4fb850bab032e5c81312cb579e2ad880372f206936db4a2be` |
| Amazon ECS | `Architecture-Service-Icons_07312026/Arch_Containers/48/Arch_Amazon-Elastic-Container-Service_48.svg` | `7f8b2883cd6171a42bdcafab2d20d17c3cf886d306c62846c6c555ad26463b04` |
| AWS Fargate | `Architecture-Service-Icons_07312026/Arch_Containers/48/Arch_AWS-Fargate_48.svg` | `2b1b00e6e702e9bd1a1cc8bba077e05f214889c145186b9a44c28a74943b38ce` |
| SQS event queue and failure queue | `Resource-Icons_07312026/Res_Application-Integration/Res_Amazon-Simple-Queue-Service_Queue_48.svg` | `39915b6e2eb2d77286a905bb0ca94912bca36605d518433db3b3f0d113e5d07d` |
| IAM task role | `Resource-Icons_07312026/Res_Security-Identity/Res_AWS-Identity-Access-Management_Role_48.svg` | `e47256f0e788e6bda40d17b0da4cf9abab170a29dfc7a22f579e13f4891407da` |
| AWS Glue Data Catalog | `Resource-Icons_07312026/Res_Analytics/Res_AWS-Glue_Data-Catalog_48.svg` | `d870d9c1e8fe9d3db69f754341438a4a366742697f6fc24f17a57aeb4a5b6b81` |
| Amazon CloudWatch Logs | `Resource-Icons_07312026/Res_Management-Governance/Res_Amazon-CloudWatch_Logs_48.svg` | `d395fbcdfe2a524b8cb8ea1f157be8f8cb02e29e635607acbfa9644a31ff4e12` |

Downloaded package SHA256: `d2d166c453526471749d520e0db022c459abef759d2946cf2dd1d1c992dc6526`.
